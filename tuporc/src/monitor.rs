use std::env::{current_dir, current_exe};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

use eyre::Result;
use fs2::FileExt;
use notify::{event, Config, Event, EventKind, RecursiveMode, Watcher};
use rusqlite::Connection;

use crate::db::{LibSqlExec, LibSqlPrepare};
use crate::parse::{AddIdsStatements, NodeStatements};
use crate::scan::scan_root;
use crate::TermProgress;

pub(crate) struct WatchObject {
    root: PathBuf,
}

impl WatchObject {
    pub fn new(root: PathBuf) -> Self {
        WatchObject { root: root.clone() }
    }
    pub fn start(&mut self) -> Result<()> {
        monitor(&self.root)?;
        Ok(())
    }
    pub fn stop(&mut self) -> Result<()> {
        stop_monitor()?;
        Ok(())
    }
}

fn fetch_latest_id(conn: &Connection, table: &str, id: &str) -> Result<i64> {
    let sql = format!("SELECT MAX({}) from {}", id, table);
    let mut stmt = conn.prepare(sql.as_str())?;
    let mut rows = stmt.query([])?;
    let row = rows.next()?.expect("no rows returned");
    let id: i64 = row.get(0)?;
    Ok(id)
}

fn fetch_latest_message(conn: &Connection) -> Result<String> {
    let message_id = fetch_latest_id(conn, "MESSAGE", "id")?;
    let message: String = conn.query_row(
        "SELECT message from Messages where id = ? ",
        [message_id],
        |r| Ok(r.get(0)?),
    )?;
    Ok(message)
}

fn monitor(root: &Path) -> Result<()> {
    let config = Config::default().with_poll_interval(Duration::from_millis(1000));
    let lock_file_path = root.join(".tup/mon_lock");
    let mut file = OpenOptions::new()
        .write(true)
        .create(true) // Create the file if it doesn't exist
        .open(lock_file_path)?;
    // Apply an exclusive lock
    file.try_lock_exclusive().map_err(|e| {
        println!("Monitor was already started");
        e
    })?;
    let (path_sender, path_receiver) = crossbeam::channel::bounded(8);
    // Write to the file
    writeln!(
        file,
        "Exclusive write access by process :{}",
        current_exe().unwrap().display()
    )?;
    println!(
        "Monitoring filesystem for changes at tup root: {}",
        current_dir()?.display()
    );

    let watch_handler = move |event: notify::Result<Event>| {
        if let Ok(event) = event {
            match event.kind {
                EventKind::Modify(event::ModifyKind::Data(_))
                | EventKind::Create(event::CreateKind::File) => {
                    for path in event.paths {
                        {
                            log::debug!("File added to list: {:?}", path);
                            path_sender.send((path, 1)).unwrap();
                        }
                    }
                }
                EventKind::Remove(event::RemoveKind::File)
                | EventKind::Remove(event::RemoveKind::Folder) => {
                    for path in event.paths {
                        log::debug!("File removed to list: {:?}", path);
                        path_sender.send((path, -1)).unwrap();
                    }
                }
                _ => {}
            }
        } else {
            log::warn!("error in event: {:?}", event.err().unwrap());
        }
    };
    crossbeam::scope(|s| -> Result<()> {
        let (stop_sender, stop_receiver) = crossbeam::channel::bounded(0);
        let stop_sender_clone = stop_sender.clone();
        ctrlc::set_handler(move || {
            stop_sender_clone
                .send(())
                .expect("Error setting Ctrl-C handler")
        })?;
        let mut conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
        let mut generation_id = fetch_latest_id(&conn, "MONITORED_FILES", "generation_id")
            .expect("unable to query generation_id");

        let mut watcher = notify::RecommendedWatcher::new(watch_handler, config)
            .expect("Failed to create watcher");
        watcher.watch(root, RecursiveMode::Recursive)?;
        let term_progress = TermProgress::new("Full scan underway..");
        let pb_main = term_progress.get_main();
        s.spawn(move |_| -> Result<()> {
            let mut current_id: i64 = 0;
            let mut end_build = true;
            scan_root(root, &mut conn, &term_progress)?;
            pb_main.println("Full scan complete");
            let mut node_statements = NodeStatements::new(&conn)?;
            let mut add_ids_statements = AddIdsStatements::new(&conn)?;
            let mut insert_monitored_prepare = conn.insert_monitored_prepare()?;

            loop {
                if end_build {
                    sleep(Duration::from_secs(10));
                } else {
                    sleep(Duration::from_secs(5));
                }
                let latest_id = fetch_latest_id(&conn, "MESSAGES", "id").unwrap_or(current_id);
                let mut end_watch = false;
                if latest_id != current_id {
                    current_id = latest_id;
                    let latest_message = fetch_latest_message(&conn)?;
                    if latest_message.eq("QUIT") {
                        end_watch = true;
                        term_progress.abandon(&pb_main, "Quit message received");
                    }
                    if latest_message.eq("STARTBUILD") {
                        generation_id = generation_id + 1;
                        end_build = false;
                    }
                    if latest_message.eq("ENDBUILD") {
                        end_build = true;
                    }
                }
                if let Ok(()) = stop_receiver.try_recv() {
                    term_progress.abandon(&pb_main, "Ctrl-c received");
                    end_watch = true;
                }

                if end_build {
                    conn.execute("DELETE * from messages", ()).unwrap();
                    if let Ok((path, added)) = path_receiver.try_recv() {
                        if added == 1 {
                            crate::parse::insert_path(
                                &path,
                                &mut node_statements,
                                &mut add_ids_statements,
                            )?;
                        } else {
                            crate::parse::remove_path(
                                &path,
                                &mut node_statements,
                                &mut add_ids_statements,
                            )?;
                        }
                    }
                } else if let Ok((path, added)) = path_receiver.try_recv() {
                    insert_monitored_prepare
                        .insert_monitored(path, generation_id, added)
                        .expect("failed to add monitored file to db");
                }

                if end_watch {
                    watcher.unwatch(root)?;
                    break;
                }
            }
            Ok(())
        });
        Ok(())
    })
    .expect("failed to spawn thread")?;
    Ok(())
}

fn stop_monitor() -> Result<()> {
    let conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
    conn.execute("INSERT INTO messages (message) VALUES ('QUIT')", [])?;
    Ok(())
}

use std::fs::File;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::thread::sleep;
use std::time::Duration;

use crossbeam::channel::internal::SelectHandle;
use notify::{event, Config, Event, EventKind, RecursiveMode, Watcher};
use rusqlite::Connection;

use crate::db::{LibSqlExec, LibSqlPrepare};
use crate::parse::{AddIdsStatements, NodeStatements};
use crate::scan::scan_root;
use crate::TermProgress;

pub fn init_db(db_name: &str) {
    println!("Creating a new db.");
    //use std::fs;
    std::fs::create_dir_all(".tup").expect("Unable to access .tup dir");
    let conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
    conn.execute_batch(include_str!("sql/node_table.sql"))
        .expect("failed to create tables in tup database.");

    let _ = File::create("Tupfile.ini").expect("could not open Tupfile.ini for write");
    println!("Finished creating tables");
}

struct WatchObject {
    root: PathBuf,
    subdir: PathBuf,
    keyword: String,
    extension: String,
    watcher: Option<dyn notify::Watcher>,
}

impl WatchObject {
    pub fn new_at_subdir_with_keyword(
        root: PathBuf,
        subdir: PathBuf,
        keyword: String,
        extension: String,
    ) -> Self {
        WatchObject {
            root,
            subdir,
            keyword,
            extension,
            watcher: None,
        }
    }
    pub fn new(root: PathBuf) -> Self {
        WatchObject {
            root: root.clone(),
            subdir: root,
            keyword: String::new(),
            extension: String::new(),
            watcher: None,
        }
    }
    pub fn start(&mut self) -> eyre::Result<()> {
        self.watcher = monitor(&self.root)?;
        Ok(())
    }
    pub fn stop(&mut self) -> eyre::Result<()> {
        if let Some(watcher) = &mut self.watcher {
            unwatch_subdir(watcher, &self.root, &self.subdir)?;
        }
        Ok(())
    }
}

fn latest_id(conn: &Connection, table: &str, id: &str) -> eyre::Result<i64> {
    let sql = format!("SELECT MAX({}) from {}", id, table);
    let mut stmt = conn.prepare(sql.as_str())?;
    let mut rows = stmt.query([])?;
    let row = rows.next()?.expect("no rows returned");
    let id: i64 = row.get(0)?;
    Ok(id)
}

fn latest_message(conn: &Connection) -> eyre::Result<String> {
    let message_id = latest_id(conn, "MESSAGE", "id")?;
    let message: String = conn.query_row(
        "SELECT message from Messages where id = ? ",
        (message_id),
        |r| Ok(r.get(0)?),
    )?;
    Ok(message)
}

fn monitor(root: &Path) -> eyre::Result<dyn notify::Watcher> {
    crossbeam::scope(|s| {
        let (stop_sender, stop_receiver) = crossbeam::channel::bounded(0);
        let stop_sender_clone = stop_sender.clone();
        ctrlc::set_handler(move || {
            stop_sender_clone
                .send(())
                .expect("Error setting Ctrl-C handler")
        })?;
        let config = Config::default().with_poll_interval(Duration::from_millis(1000));
        let mut conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
        let mut generation_id = latest_id(&conn, "MONITORED_FILES", "generation_id")
            .expect("unable to query generation_id");
        let mut ins_prepare = conn.insert_monitored_metadata_prepare()?;
        let mut fetch_metadata_prepare = conn.fetch_monitored_metadata_prepare()?;
        let current_time = std::time::SystemTime::now();
        let time_since_epoch = current_time
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards");

        let mut insert_monitored_prepare = conn.insert_monitored_prepare()?;
        let mut remove_monitored_prepare = conn.remove_monitored_metadata_prepare()?;
        let (path_sender, path_receiver) = crossbeam::channel::bounded(8);
        let handler = |event: notify::Result<Event>| {
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
                    Event::Remove(event::RemoveKind::File()) => {
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
        let mut watcher = notify::Watcher::new(handler, config).expect("Failed to create watcher");
        watcher.watch(root, RecursiveMode::Recursive).unwrap();

        s.spawn(move |_| -> eyre::Result<()> {
            let mut currentId: i64 = 0;
            let mut end_build = true;
            scan_root(root, &mut conn, &TermProgress::new("Scanning.."))?;
            let mut node_statements = NodeStatements::new(conn.deref())?;
            let mut add_ids_statements = AddIdsStatements::new(conn.deref())?;

            loop {
                if end_build {
                    sleep(Duration::from_secs(10));
                } else {
                    sleep(Duration::from_secs(5));
                }
                let latestId = latest_id(&conn, "MESSAGES", "id").unwrap_or(currentId);
                if latestId != currentId {
                    currentId = latestId;
                    let latestMessage = latest_message(&conn)?;
                    if latestMessage.eq("QUIT") {
                        break;
                    }
                    if latestMessage.eq("STARTBUILD") {
                        generation_id = generation_id + 1;
                        end_build = false;
                    }
                    if latestMessage.eq("ENDBUILD") {
                        end_build = true;
                    }
                }

                if end_build {
                    conn.execute("DELETE * from messages", ()).unwrap();
                    if let Ok((path, added)) = path_receiver.try_recv() {
                        if (added) {
                            crate::parse::insert_path(
                                &mut conn,
                                &path,
                                &mut node_statements,
                                &mut add_ids_statements,
                            )?;
                        } else {
                            crate::parse::remove_path(&mut conn, &path, &mut node_statements)?;
                        }
                    }
                } else if let Ok((path, added)) = path_receiver.try_recv() {
                    insert_monitored_prepare
                        .insert_monitored(path, generation_id, added)
                        .expect("failed to add monitored file to db");
                }
            }
            Ok(())
        });
    })?;
    Ok(watcher)
}

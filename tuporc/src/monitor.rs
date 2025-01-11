use std::env::{current_dir, current_exe};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use tupdb::db::{RowType};
use crate::parse::{CrossRefMaps};
use crate::scan::scan_root;
use crate::TermProgress;
use crossbeam::channel::Receiver;
use eyre::{Report, Result};
use fs2::FileExt;
use ignore::gitignore::Gitignore;
use indicatif::ProgressBar;
use notify::{
    event, Config, Event, EventKind, ReadDirectoryChangesWatcher, RecursiveMode, Watcher,
};
use rusqlite::Connection;
use tupdb::inserts::LibSqlInserts;
use tupdb::queries::LibSqlQueries;
use tupparser::buffers::{BufferObjects, PathBuffers};

pub(crate) struct WatchObject {
    root: PathBuf,
    ign: Gitignore,
}

impl WatchObject {
    pub fn new(root: PathBuf, ign: Gitignore) -> Self {
        WatchObject {
            root: root.clone(),
            ign,
        }
    }
    pub fn start(&mut self) -> Result<()> {
        monitor(&self.root, self.ign.clone())?;
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

fn fetch_latest_ids(conn: &Connection, table: &str, id: &str, current_id: i64) -> Result<Vec<i64>> {
    let sql = format!("SELECT {id} from {table} where {id} > {current_id}");
    let mut stmt = conn.prepare(sql.as_str())?;
    let mut rows = stmt.query([])?;
    let mut ids = Vec::new();
    while let Some(row) = rows.next()? {
        let id: i64 = row.get(0)?;
        ids.push(id);
        //Ok(id)
    }
    Ok(ids)
}

fn fetch_message(conn: &Connection, message_id: i64) -> Result<String> {
    let message: String = conn.query_row(
        "SELECT message from Messages where id = ? ",
        [message_id],
        |r| Ok(r.get(0)?),
    )?;
    Ok(message)
}

fn is_file_locked_for_write<P: AsRef<Path>>(path: P) -> Result<bool> {
    let file = OpenOptions::new().read(true).open(path)?;
    Ok(file.try_lock_exclusive().is_err())
}

fn monitor(root: &Path, ign: ignore::gitignore::Gitignore) -> Result<()> {
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
    let (path_sender, path_receiver) = crossbeam::channel::unbounded();
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
    let root = current_dir()?;
    let root_sz = root.components().count();
    let watch_handler = move |e: notify::Result<Event>| {
        if let Ok(event) = e {
            match event.kind {
                EventKind::Modify(_) | EventKind::Create(event::CreateKind::File) => {
                    for path in event.paths.into_iter() {
                        if path
                            .file_name()
                            .iter()
                            .find(|&&item| item.eq(std::ffi::OsStr::new("db-wal")))
                            .is_some()
                        {
                            continue;
                        }
                        if !is_ignorable(&path, &ign, false) {
                            log::debug!("File added to list: {:?}", path);
                            path_sender
                                .send((path.into_iter().skip(root_sz).collect::<PathBuf>(), 1))
                                .unwrap();
                        }
                    }
                }
                EventKind::Remove(event::RemoveKind::File) => {
                    for path in event.paths.into_iter() {
                        if !is_ignorable(&path, &ign, false) {
                            log::debug!("File removed to list: {:?}", path);
                            path_sender
                                .send((path.into_iter().skip(root_sz).collect::<PathBuf>(), 0))
                                .unwrap();
                        }
                    }
                }
                EventKind::Remove(event::RemoveKind::Folder) => {
                    for path in event.paths.into_iter() {
                        if !is_ignorable(&path, &ign, true) {
                            log::debug!("File removed to list: {:?}", path);
                            path_sender
                                .send((path.into_iter().skip(root_sz).collect::<PathBuf>(), 0))
                                .unwrap();
                        }
                    }
                }
                _ => {}
            }
        } else {
            log::warn!("error in event: {:?}", e.err().unwrap());
        }
    };
    let running = Arc::new(AtomicBool::new(true));
    let (stop_sender, stop_receiver) = crossbeam::channel::bounded(0);
    let stop_sender_clone = stop_sender.clone();
    {
        let running = running.clone();
        let _ = ctrlc::try_set_handler(move || {
            let _ = stop_sender_clone.send(());
            running.store(false, std::sync::atomic::Ordering::Relaxed);
        })
        .map_err(|e| {
            log::error!("Failed to set handler: {}", e);
        });
    }
    let mut conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
    let term_progress = TermProgress::new("Full scan underway..");
    scan_root(root.as_path(), &mut conn, &term_progress, running.clone())?;
    crossbeam::scope(|s| -> Result<()> {
        let generation_id = fetch_latest_id(&conn, "MONITORED_FILES", "generation_id").unwrap_or(1);
        let mut watcher = notify::RecommendedWatcher::new(watch_handler, config)
            .expect("Failed to create watcher");
        watcher.watch(root.as_path(), RecursiveMode::Recursive)?;
        let pb_main = term_progress.get_main();
        let _ = s
            .spawn(move |_| -> Result<()> {
                run_monitor(
                    path_receiver,
                    root,
                    &mut conn,
                    term_progress,
                    stop_receiver,
                    generation_id,
                    watcher,
                    pb_main,
                )
            })
            .join()
            .expect("failed to join thread")?;
        Ok(())
    })
    .expect("failed to spawn thread")?;
    Ok(())
}

fn run_monitor(
    path_receiver: Receiver<(PathBuf, i32)>,
    root: PathBuf,
    conn: &Connection,
    term_progress: TermProgress,
    stop_receiver: Receiver<()>,
    mut generation_id: i64,
    mut watcher: ReadDirectoryChangesWatcher,
    pb_main: ProgressBar,
) -> Result<()> {
    let current_id: i64 = 0;
    let mut build_in_progess = false;
    pb_main.println("Full scan complete");
    let pb = term_progress.pb_main.clone();
    let pb = pb.with_message("Monitoring filesystem for changes");
    tupdb::db::create_path_buf_temptable(conn)?;
    let bo = BufferObjects::new(root);
    let mut cross_ref_maps = CrossRefMaps::default();
    let mut update_nodes = |path: &Path, added: bool| -> Result<()> {
        if added {
            let pd = bo
                .add_abs(path)
                .expect("failed to add path to buffer objects");
            crate::parse::insert_path(
                conn,
                &bo,
                &pd,
                &mut cross_ref_maps,
                RowType::File,
            )?;
        } else {
            crate::parse::remove_path(conn, &path)?;
        }
        Ok(())
    };
    loop {
        sleep(Duration::from_secs(5));
        pb.tick();
        let end_watch = if let Ok(()) = stop_receiver.try_recv() {
            term_progress.abandon(&pb, "Ctrl-c received");
            true
        } else {
            poll_for_new_messages(conn, &term_progress, current_id, &pb)?
        };
        if end_watch {
            watcher.unwatch(bo.get_root_dir())?;
            break;
        }
        let build_in_progess_new_stat =
            is_file_locked_for_write(".tup/build.lock").unwrap_or(false);
        if build_in_progess != build_in_progess_new_stat {
            if !build_in_progess_new_stat {
                let monitored_files = conn.fetch_monitored_files(generation_id)?;
                conn.execute("DELETE from MONITORED_FILES", ())?;
                for (path, added) in monitored_files {
                    update_nodes(&Path::new(path.as_str()), added)?;
                }
            }
            generation_id += 1;
            build_in_progess = build_in_progess_new_stat;
        }
        while let Ok((path, added)) = path_receiver.try_recv() {
            if build_in_progess_new_stat {
                conn.insert_monitored(path.as_path().to_string_lossy().as_ref(), generation_id, added as _)
                    .expect("failed to add monitored file to db");
            } else {
                update_nodes(&path, added == 1)?;
            }
        }
    }
    Ok(())
}

fn poll_for_new_messages(
    conn: &Connection,
    term_progress: &TermProgress,
    mut current_id: i64,
    pb: &ProgressBar,
) -> Result<bool, Report> {
    let mut end_watch = false;
    let latest_ids = fetch_latest_ids(&conn, "MESSAGES", "id", current_id).unwrap_or(Vec::new());
    for latest_id in latest_ids.iter() {
        current_id = *latest_id;
        let latest_message = fetch_message(&conn, current_id)?;
        if latest_message.eq("QUIT") {
            end_watch = true;
            term_progress.abandon(&pb, "Quit message received");
            break;
        }
    }
    if !latest_ids.is_empty() {
        conn.execute("DELETE from messages", ())?;
    }
    Ok(end_watch)
}

fn is_ignorable<P: AsRef<Path>>(path: P, ign: &ignore::gitignore::Gitignore, is_dir: bool) -> bool {
    return ign.matched(path.as_ref(), is_dir).is_ignore();
}

fn stop_monitor() -> Result<()> {
    let conn =
        Connection::open(".tup/db").expect("Failed to connect to .tup/db. Are you in a tup root?");
    conn.execute("INSERT INTO messages (message) VALUES ('QUIT')", [])?;
    Ok(())
}

pub(crate) fn is_monitor_running() -> bool {
    let lock_file_path = current_dir().unwrap().join(".tup/mon_lock");
    is_file_locked_for_write(lock_file_path).unwrap_or(false)
}

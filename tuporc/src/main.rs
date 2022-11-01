mod db;
mod parse;

use anyhow::Result;
use db::ForEachClauses;
use jwalk::Parallelism;
use jwalk::WalkDir;
use std::collections::hash_map::Entry::Occupied;
use std::collections::{HashMap, HashSet};
use std::env::current_dir;
use std::ffi::OsStr;
use std::fs;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

extern crate clap;
extern crate bimap;
extern crate num;
#[macro_use]
extern crate num_derive;
use crate::db::{
    init_db, is_initialized, LibSqlExec, LibSqlPrepare, SqlStatement,
};
use crate::parse::parse_tupfiles_in_db;
use clap::Parser;
use db::RowType::{DirType, GrpType};
use db::{Node, RowType};
use rusqlite::{Connection, Row};

#[derive(clap::Parser)]
#[clap(author, version = "0.1", about = "Tup build system implemented in rust", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Option<Action>,
    /// Verbose output of the build steps
    #[clap(long)]
    verbose: bool,
}

#[derive(clap::Subcommand)]
enum Action {
    #[clap(about = "Creates a tup database")]
    Init,

    #[clap(about = "Scans the file system for changes since the last scan")]
    Scan,

    #[clap(about = "Parses the tup files in a tup database")]
    Parse,

    #[clap(about = "Build specified targets")]
    Upd {
        /// Space separated targets to build
        target: Vec<String>,
    },
}

fn is_tupfile(s: &OsStr) -> bool {
    s == "Tupfile" || s == "Tupfile.lua"
}

fn make_node(row: &Row) -> rusqlite::Result<Node> {
    let id: i64 = row.get(0)?;
    let pid: i64 = row.get(1)?;
    let mtime: i64 = row.get(2)?;
    let name: String = row.get(3)?;
    let rtype: i8 = row.get(4)?;
    let rtype = match rtype {
        0 => RowType::FileType,
        1 => RowType::RuleType,
        2 => DirType,
        3 => RowType::EnvType,
        4 => RowType::GenFType,
        5 => RowType::TupFType,
        6 => GrpType,
        7 => RowType::GEndType,
        _ => panic!("Invalid type {} for row with id:{}", rtype, id),
    };
    Ok(Node::new(id, pid, mtime, name, rtype))
}

fn main() -> Result<()> {
    let args = Args::parse();

    if let Some(act) = args.command {
        match act {
            Action::Init => {
                init_db();
            }
            Action::Scan => {
                let mut conn = Connection::open(".tup/db")
                    .expect("Connection to tup database in .tup/db could not be established");
                if !is_initialized(&conn) {
                    return Err(anyhow::Error::msg(
                        "Tup database is not initialized, use `tup init' to initialize",
                    ));
                }
                println!("Scanning for files");
                let root = current_dir()?;
                let mut present: HashSet<i64> = HashSet::new(); // tracks files/folder still in the filesystem
                match scan_root(root.as_path(), &mut conn, &mut present) {
                    Err(e) => eprintln!("{}", e.to_string()),
                    Ok(()) => println!("Scan was successful"),
                };
            }
            Action::Parse => {
                let mut conn = Connection::open(".tup/db")
                    .expect("Connection to tup database in .tup/db could not be established");
                if !is_initialized(&conn) {
                    return Err(anyhow::Error::msg(
                        "Tup database is not initialized, use `tup init' to initialize",
                    ));
                }
                let root = current_dir()?;
                println!("Parsing tupfiles in database");
                let mut present: HashSet<i64> = HashSet::new(); // tracks files/folder still in the filesystem
                scan_root(root.as_path(), &mut conn, &mut present)?;
                parse_tupfiles_in_db(&mut conn, root.as_path())?;
                delete_missing(&conn, &present)?;
            }
            Action::Upd { target } => {
                println!("Updating db {}", target.join(" "));
            }
        }
    }
    println!("Done");
    Ok(())
}

/// handle the tup scan command by walking the directory tree and adding dirs and files into node table.
fn scan_root(root: &Path, conn: &mut Connection, present: &mut HashSet<i64>) -> Result<()> {
    insert_direntries(root, present, conn)
}

// WIP... delete files and rules in db that arent in the filesystem or in use
fn delete_missing(conn: &Connection, present: &HashSet<i64>) -> Result<()> {
    let mut delete_stmt = conn.delete_prepare()?;
    let mut delete_aux_stmt = conn.delete_aux_prepare()?;

    conn.for_each_file_node_id(|node_id: i64| -> Result<()> {
        if !present.contains(&node_id) {
            //XTODO: delete rules and generated files derived from this id
            delete_stmt.delete_exec(node_id)?;
            delete_aux_stmt.delete_exec_aux(node_id)?;
        }
        Ok(())
    })?;
    Ok(())
}

/// return dir id either from db stored value in readstate or from newly created list in created dirs
pub (crate) fn get_dir_id<P: AsRef<Path>>(dirs_in_db: &mut SqlStatement, path: P) -> Option<i64> {
    dirs_in_db.fetch_dirid(path).ok() // check if in db already
}

/// mtime stored wrt 1-1-1970
fn time_since_unix_epoch(curpath: &Path) -> Result<Duration, Error> {
    let meta_data = fs::metadata(curpath)?;
    let st = meta_data.modified()?;
    Ok(st
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0)))
}

/// insert directory entries into Node table if not already added.
fn insert_direntries(root: &Path, present: &mut HashSet<i64>, conn: &mut Connection) -> Result<()> {
    //let mut insert_dir_aux = conn.insert_dir_aux_prepare()?;
    // search parent dir id with relative path from root
    {
        // insert / fetch root node in db
        //let  dirnodes = conn.fetch_nodes_prepare()?;
        let existing_node = conn.fetch_node_prepare()?.fetch_node(".", 0).ok();
        let n = existing_node.map(|n| n.get_id());
        if n.is_none() {
            let mut insert_dir = conn.insert_dir_prepare()?;
            let id = insert_dir.insert_dir_exec(".", 0)?;
            anyhow::ensure!(id == 1, format!("unexpected id for root dir :{} ", id));
            present.insert(id);
        }
    }
    let mut parent_ids: HashMap<PathBuf, i64> = HashMap::new();
    parent_ids.insert(root.to_path_buf(), 1);
    for e in WalkDir::new(root)
        .follow_links(true)
        .parallelism(Parallelism::RayonDefaultPool)
        .skip_hidden(true)
        .process_read_dir(move |_, _, _, children| {
            children.retain(|d| {
                d.as_ref()
                    .map_or(false, |direntry| direntry.path().is_dir())
            });
        })
        .into_iter().filter_map(|e| e.ok())
    {
        let maybe_id = parent_ids.entry(e.path());
        let pid: i64;
        if let Occupied(o) = maybe_id {
            pid = *o.get();
            o.remove_entry();
        } else {
            return Err(anyhow::Error::msg(format!(
                "Could not find a valid id for dir:{:?}",
                e.path()
            )));
        }
        let existing_nodes = conn.fetch_nodes_prepare()?.fetch_nodes([pid])?;
        println!("{}", e.path().to_string_lossy().to_string());
        let tx = conn.transaction()?;
        {
            let mut insert_new_node = tx.insert_node_prepare()?;
            let mut update_mtime = tx.update_mtime_prepare()?;
            let mut add_to_modified_list = tx.add_to_modify_prepare()?;

            let mut insert_dir = tx.insert_dir_prepare()?;

            let curdir = e.path();
            for file_entry in WalkDir::new(curdir)
                .follow_links(true)
                .skip_hidden(true)
                .max_depth(1) // walk to immediate children only
                .min_depth(1)
            // skip curdir
            {
                if let Ok(f) = file_entry {
                    let path_str = f.file_name().to_string_lossy().to_string();
                    let cur_path = f.path();
                    let n = existing_nodes
                        .iter()
                        .find(|node| node.get_name() == path_str && node.get_pid() == pid);
                    n.map(|node| present.insert(node.get_id()));
                    if f.path().is_file() {
                        if let Ok(mtime) = time_since_unix_epoch(cur_path.as_path()) {
                            if let Some(n) = n {
                                // for a node already in db, check the diffs in mtime
                                let curtime = mtime.subsec_nanos() as i64;
                                if n.get_mtime() != curtime {
                                    update_mtime.update_mtime_exec(curtime, n.get_id())?;
                                    //add_to_modified_list.add_to_modify_exec(n.get_id())?;
                                }
                            } else {
                                // otherwise insert node in db
                                let rtype = if is_tupfile(f.file_name()) {
                                    RowType::TupFType
                                } else {
                                    RowType::FileType
                                };
                                let node =
                                    Node::new(0, pid, mtime.subsec_nanos() as i64, path_str, rtype);
                                let id = insert_new_node.insert_node_exec(&node)?;
                                // add newly created nodes also into modified list
                                if rtype == RowType::TupFType {
                                    add_to_modified_list.add_to_modify_exec(id)?;
                                }
                            }
                        }
                    } else if f.path().is_dir() {
                        let id = if let Some(node) = n {
                            node.get_id()
                        } else {
                            insert_dir.insert_dir_exec(path_str.as_str(), pid)?
                        };
                        parent_ids.insert(f.path(), id);
                        present.insert(id);
                    }
                }
            }
        }
        tx.commit()?;
    }
    Ok(())
}

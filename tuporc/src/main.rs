mod db;
mod parse;

use anyhow::Result;
use jwalk::Parallelism;
use jwalk::WalkDir;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsStr;
use std::fs;
use std::fs::File;
use std::io::{Error, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

extern crate clap;
use clap::Parser;
use db::{Node, RowType};
use rusqlite::{Connection, Params, Row, Statement};

use crate::db::{init_db, is_initialized, LibSqlExec, LibSqlPrepare, SqlStatement};
use db::RowType::{DirType, GrpType};
//use tupparser::statements::Link as Lnk;
//use tupparser::*;

#[derive(clap::Parser)]
#[clap(author, version="0.1", about="Tup build system", long_about = None)]
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

/// Read the node table and fill up a map between Path and the node
fn query(conn: &Connection, pathid: &mut HashMap<PathBuf, Node>) -> Result<()> {
    let mut stmt = conn.prepare("SELECT id, dir, name, mtime_ns, type FROM Node")?;
    query_with_stmt(pathid, &mut stmt, [])
}

fn query_row_with_stmt<P: Params>(stmt: &mut Statement, params: P) -> Result<Node> {
    let res = stmt.query_row(params, |r| make_node(r))?;
    Ok(res)
}

fn query_with_stmt<P: Params>(
    pathid: &mut HashMap<PathBuf, Node>,
    stmt: &mut Statement,
    params: P,
) -> Result<()> {
    let mut rows = stmt.query(params)?;
    let mut namemap = HashMap::new();
    while let Some(row) = rows.next()? {
        let node = make_node(row)?;
        namemap.insert(node.get_id(), node);
    }
    //let mut pathid = HashMap::new();
    for (k, v) in namemap.iter() {
        //parts.clear();
        let mut parts = std::collections::VecDeque::new();
        parts.push_front(v.get_name().clone());
        while let Some(n) = namemap.get(k) {
            if let Some(v) = namemap.get(&n.get_pid()) {
                parts.push_front(v.get_name().clone());
            } else {
                break;
            }
        }
        let mut pb = PathBuf::from(".");
        for p in parts.iter() {
            pb = pb.join(Path::new(p));
        }
        pathid.insert(pb, v.clone());
    }
    Ok(())
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
        4 => RowType::GEnFType,
        5 => RowType::TupFType,
        6 => GrpType,
        7 => RowType::GEndType,
        _ => panic!("Invalid type {} for row with id:{}", rtype, id),
    };
    Ok(Node::new(id, pid, mtime, name, rtype))
}

fn main() {
    let args = Args::parse();

    if let Some(act) = args.command {
        match act {
            Action::Init => {
                init_db();
            }
            Action::Scan => {
                let ref conn = Connection::open(".tup/db")
                    .expect("Connection to tup database in .tup/db could not be established");
                if !is_initialized(conn) {
                    eprintln!("Tup database is not initialized, use `tup init' to initialize");
                    return;
                }
                println!("Scanning for files");
                let root = Path::new("c:/users/aruns/tupsprites");
                match scan_root(root, conn) {
                    Err(e) => eprintln!("{}", e.to_string()),
                    Ok(()) => println!("Scan was successful"),
                };
            }
            Action::Parse => {
                println!("Parsing tupfiles in database");
            }
            Action::Upd { target } => {
                println!("Updating db {}", target.join(" "));
            }
        }
    }
    println!("Done");
}

/// handle the tup scan command by walking the directory tree and adding dirs and files into node table.
fn scan_root(root: &Path, conn: &Connection) -> Result<()> {
    //we insert directories in node table first because we need their ids in file and subdir rows
    let mut present: HashSet<i64> = HashSet::new(); // tracks files/folder still in the filesystem
    insert_directories(root, &mut present, conn)?;
    insert_files(root, &mut present, conn)?;
    let mut delete_stmt = conn.delete_prepare()?;
    let mut delete_aux_stmt = conn.delete_aux_prepare()?;

    conn.foreach_fnode_id(|nodeid:i64|
        {
            if !present.contains(&nodeid) {
                //TODO: delete rules and generated files derived from this id
                delete_stmt.delete_exec(nodeid)?;
                delete_aux_stmt.delete_exec_aux(nodeid)?;
            }
            Ok(())
        }
    );
    Ok(())
}

/// insert files into Node table if not already added. It is expected that the directories in which these files appear have already been added.
/// For subdirs only the parent dir id is updated
fn insert_files(
    root: &Path,
    present: &mut HashSet<i64>,
    conn: &Connection,
) -> Result<()> {
    let mut insert_new_node = conn.insert_node_prepare()?;
    let mut update_dir_id = conn.update_dirid_prepare()?;
    let mut update_mtime = conn.update_mtime_prepare()?;
    let mut add_to_modified_list = conn.add_to_modify_prepare()?;
    let mut dirs_in_db = conn.find_dirid_prepare()?;
    let dirid = |e: &Path| get_dir_id( &mut dirs_in_db, e);

    for d in WalkDir::new(root)
        .follow_links(true)
        .parallelism(Parallelism::RayonDefaultPool)
        .skip_hidden(true)
    {
        if let Ok(e) = d {
            let parentpath = e.parent_path();
            let ref curpath = e.path();
            let n = readstate.get(curpath.as_path());
            n.map(|n| present.insert(n.get_id()));

            if e.path().is_file() {
                let pathstr = e.file_name.to_string_lossy().to_string();
                let m = time_since_unix_epoch(curpath);
                if let Ok(mtime) = m {
                    if let Some(n) = n {
                        // for a node already in db, check the diffs in mtime
                        let curtime = mtime.subsec_nanos() as i64;
                        if n.get_mtime() != curtime {
                            update_mtime.update_mtime_exec(curtime, n.get_id())?;
                            add_to_modified_list.add_to_modify_exec(n.get_id())?;
                        }
                    } else if let Some(dirnode) = dirid(parentpath) {
                        // otherwise insert a new node with parent dir id stored either in readstate or createdirs
                        let rtype = if is_tupfile(e.file_name()) {
                            RowType::TupFType
                        } else {
                            RowType::FileType
                        };
                        let node = Node::new(0, dirnode, mtime.subsec_nanos() as i64, pathstr, rtype);
                        insert_new_node.insert_node_exec(node)?;
                        // add newly created nodes also into modified list
                        add_to_modified_list.add_to_modify_exec(dirnode)?;
                    }
                }
            } else {
                // for a directory assign its parent id,
                if let (Some(node), Some(parent)) = (dirid(curpath.as_path()), dirid(parentpath)) {
                    update_dir_id.update_dirid_exec(parent, node)?;
                }
            }
        }
    }
    Ok(())
}


/// return dir id either from db stored value in readstate or from newly created list in createddirs
fn get_dir_id<P: AsRef<Path>>(
    //readstate: &HashMap<PathBuf, Node>,
    dirs_in_db: &mut SqlStatement,
    pbuf: P,
) -> Option<i64> {
    dirs_in_db.fetch_dirid(pbuf).ok() // check if in db already
}

/// mtime stored wrt 1-1-1970
fn time_since_unix_epoch(curpath: &Path) -> Result<Duration, Error> {
    let meta_data = fs::metadata(curpath)?;
    let st = meta_data.modified()?;
    Ok(st
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0)))
}

/// insert directories into Node table if not already added.
fn insert_directories(
    root: &Path,
    present: &mut HashSet<i64>,
    conn: &Connection,
) -> Result<()> {
    let mut dirs_in_db = conn.find_dirid_prepare()?;
    let mut insert_dir = conn.insert_dir_prepare()?;
    let mut insert_dir_aux = conn.insert_dir_aux_prepare()?;
    let dirid = |e: &Path| get_dir_id( &mut dirs_in_db, e);

    for d in WalkDir::new(root)
        .follow_links(true)
        .parallelism(Parallelism::RayonDefaultPool)
        .skip_hidden(true)
        .process_read_dir(move |_, _, _, children| {
            children.retain(|d| {
                d.as_ref()
                    .map_or(false, |direntry| direntry.path().is_dir())
            });
        })
    {
        if let Ok(e) = d {
            let pathstr = e.file_name.to_string_lossy().to_string();
            let n = dirid(e.path().as_path());
            n.map(|i| present.insert(i));
            if n.is_none() {
                let id = insert_dir.insert_dir_exec(pathstr.as_str())?;
                insert_dir_aux.insert_dir_aux_exec(id, e.path())?;
                println!("{}", e.path().to_string_lossy().to_string());
            }
        }
    }
    Ok(())
}

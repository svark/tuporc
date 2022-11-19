extern crate bimap;
extern crate clap;
extern crate crossbeam;
extern crate num;
#[macro_use]
extern crate num_derive;

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::env::current_dir;
use std::ffi::{OsStr, OsString};
use std::fs::{FileType, Metadata};
use std::io::Error;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::Arc;
use std::thread::yield_now;
use std::time::{Duration, SystemTime};
use std::vec::Drain;

use anyhow::Result;
use clap::Parser;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use crossbeam::thread;
use jwalk::DirEntry;
use jwalk::WalkDir;
use rusqlite::{Connection, Row};

use db::{Node, RowType};
use db::ForEachClauses;
use db::RowType::{Dir, Grp};

use crate::db::{init_db, is_initialized, LibSqlExec, LibSqlPrepare, SqlStatement};
use crate::parse::{find_upsert_node, parse_tupfiles_in_db};

mod db;
mod parse;

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
        0 => RowType::File,
        1 => RowType::Rule,
        2 => Dir,
        3 => RowType::Env,
        4 => RowType::GenF,
        5 => RowType::TupF,
        6 => Grp,
        7 => RowType::GEnd,
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
                    Err(e) => eprintln!("{}", e),
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
// should restrict attention to the outputs of tupfiles that are modified/deleted.
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
pub(crate) fn get_dir_id<P: AsRef<Path>>(dirs_in_db: &mut SqlStatement, path: P) -> Option<i64> {
    dirs_in_db.fetch_dirid(path).ok() // check if in db already
}

/// mtime stored wrt 1-1-1970
fn time_since_unix_epoch(meta_data: &Metadata) -> Result<Duration, Error> {
    let st = meta_data.modified()?;
    Ok(st
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0)))
}

#[derive(PartialEq, Clone, Debug)]
struct DirE {
    file_type: FileType,
    file_name: OsString,
}

impl DirE {
    pub fn from(de:&DirEntry<((), ())>) -> DirE {
        DirE {
            file_type: de.file_type(),
            file_name: de.file_name().to_owned(),
        }
    }

    pub fn path(&self, parent: &Path) -> PathBuf {
        parent.join(&self.file_name)
    }

    pub fn file_type(&self) -> &FileType {
        &self.file_type
    }

    pub fn file_name(&self) -> &OsStr {
        self.file_name.as_os_str()
    }
}


#[derive(Debug)]
struct PayLoad {
    depth : usize,
    parent_path: Arc<PathBuf>,
    children: Vec<DirE>,
}

impl PayLoad {
    pub fn new(
        depth: usize,
        parent_path: Arc<PathBuf>,
        child: &Vec<jwalk::Result<DirEntry<((), ())>>>,
    ) -> PayLoad {
        PayLoad {
            depth,
            parent_path,
            children: child
                .iter()
                .filter_map(|e| e.as_ref().ok())
                .map(DirE::from)
                .collect(),
        }
    }

    fn parent_path(&self) -> &Path {
        self.parent_path.deref().as_path()
    }
    fn get_children(&mut self) -> Drain<'_, DirE> {
        self.children.drain(..)
    }
    fn get_depth(&self) -> usize {
        self.depth
    }
}
#[derive(Clone, Debug, PartialEq)]
struct ProtoNode {
    p: DirE,
    pid: i64,
    pbuf: Arc<PathBuf>,
}

impl PartialEq<Self> for PayLoad {
    fn eq(&self, other: &Self) -> bool {
        self.parent_path.deref() == (other.parent_path.deref())
    }
}

impl PartialOrd for PayLoad {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.get_depth().partial_cmp(&other.get_depth())
    }
}


impl Eq for PayLoad {}

impl Ord for PayLoad {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.parent_path().deref().cmp(other.parent_path().deref())
    }
}

/// insert directory entries into Node table if not already added.
fn insert_direntries(root: &Path, present: &mut HashSet<i64>, conn: &mut Connection) -> Result<()> {
    println!("Sqlite version: {}\n", rusqlite::version());
    {
        let existing_node = conn.fetch_node_prepare()?.fetch_node(".", 0).ok();
        let n = existing_node.map(|n| n.get_id());
        if n.is_none() {
            let mut insert_dir = conn.insert_dir_prepare()?;
            let id = insert_dir.insert_dir_exec(".", 0)?;
            anyhow::ensure!(id == 1, format!("unexpected id for root dir :{} ", id));
            present.insert(id);
        }
    }


    let (nodesender, nodereceiver) = crossbeam::channel::unbounded();
    let (dire_sender, dire_receiver) = crossbeam::channel::unbounded::<ProtoNode>();
    let process_dir = |mut payload: PayLoad, pid: i64, ds: &Sender<ProtoNode>|
     -> Result<()> {
        //let curdir = payload.dir.as_path();
        //let existing_nodes = conn.fetch_nodes_prepare_by_dirid()?.fetch_nodes_by_dirid([pid])?;
        //println!("processing dir {:?} with id: {}",  payload.parent_path(), pid);
        // skip curdir
        let pp = payload.parent_path.clone();
        for p in payload.get_children(){
            //let path_str = f.file_name().to_string_lossy().to_string();
            ds.send(ProtoNode {p, pid, pbuf: pp.clone()}).unwrap();
        }
        //tx.commit()?;
        Ok(())
    };

    //let (diridsender, dirid_receiver) = crossbeam::channel::unbounded::<(PathBuf, i64)>();
    thread::scope(|s| {
        let heap = Arc::new(RwLock::new(std::collections::binary_heap::BinaryHeap::<Reverse<PayLoad> >::new()));
        let  dir_id_by_path = Arc::new(RwLock::new(HashMap::new()));
        let  (payloadsender, payloadreceiver) = crossbeam::channel::unbounded();
        dir_id_by_path.write().unwrap().insert(root.to_path_buf(), 1);
        println!("root:{:?}", root.to_path_buf());
        //let heapsize = AtomicUsize::new(0);
        {
            let  payloadreceiverclone : Receiver<PayLoad>= payloadreceiver.clone();
            let  heapclone = heap.clone();
            let  dir_id_by_path_clone = dir_id_by_path.clone();
            s.spawn(move |_| -> Result<()> {
                loop{
                    match payloadreceiverclone.try_recv(){
                        Ok(payload) => {
                            if let Ok(mut heaprefmut) = heapclone.write()
                            {
                               // println!("received payload :{:?}", payload.parent_path());
                                heaprefmut.push(Reverse(payload));
                                //println!(" hx {:?}", heapsize.load(Ordering::SeqCst));
                            }else {
                                return Err(anyhow::Error::msg("heap access failure"));
                            }
                        }
                        Err(TryRecvError::Disconnected) => {
                            if let Ok(heapref) = heapclone.read()
                            {
                                if heapref.is_empty()
                                {
                                    break;
                                }
                            }
                        }
                        Err(_) => {}
                    }
                    {
                        if let Ok(diridreader) = dir_id_by_path_clone.read() {
                            if let Ok(mut heapref) = heapclone.write()
                            {
                                if let Some(Reverse(payload)) = heapref.pop() {
                                    //println!(" try {:?} ", payload.parent_path());
                                    if let Some(pid) = diridreader.deref().get(payload.parent_path()) {
                                        let pid = *pid;
                                        process_dir(payload, pid, &dire_sender)?;
                                        //println!(" hx {:?}", heapref.len());
                                    }else {

                                       // println!("mishit :{:?}", payload.parent_path.as_path());
                                        heapref.push(Reverse(payload));
                                    }
                                }
                            }
                        }
                        yield_now();

                    }
                }
                drop(dire_sender);
                Ok(())
            });

        }
        {
            for _ in 0..12 {
               let ns = nodesender.clone();
               let dire_receiver = dire_receiver.clone();
               s.spawn( move |_| -> Result<()> {
                   loop {
                       match dire_receiver.try_recv() {
                           Ok(p) => {
                               let f = p.p;
                               let pid = p.pid;
                               if f.file_type().is_file() {
                                   let fpath = f.path(p.pbuf.as_ref());
                                   let metadata = fpath.metadata()?;
                                   if let Ok(mtime) = time_since_unix_epoch(&metadata) {
                                       // for a node already in db, check the diffs in mtime
                                       // otherwise insert node in db
                                       let rtype = if is_tupfile(f.file_name()) {
                                           RowType::TupF
                                       } else {
                                           RowType::File
                                       };
                                       let node = Node::new(
                                           0,
                                           pid,
                                           mtime.subsec_nanos() as i64,
                                           fpath.to_string_lossy().to_string(),
                                           rtype,
                                       );
                                       ns.send(node).expect("Failed to send node");
                                       // let id = find_upsert_node(&mut insert_new_node, &mut find_node, &mut update_mtime, &node)?.get_id();
                                       // present.insert(id);
                                   }
                               } else if f.file_type().is_dir() {
                                   let fpath = f.path(p.pbuf.as_ref()).to_string_lossy().to_string();
                                   let node = Node::new(0, pid, 0, fpath, Dir);
                                   ns.send(node).expect("Failed to send node");
                                   //let id = find_upsert_node(&mut insert_new_node, &mut find_node, &mut update_mtime, &node)?.get_id();
                                   //dir_id_by_path.insert(f.path(), id);
                                   //present.insert(id);
                               }
                           },
                           Err(TryRecvError::Disconnected) => {break},
                           Err(_) => {}
                       }
                       yield_now();
                   }
                   drop(ns);
                   Ok(())
               }) ;
            }
            drop(nodesender);
        }
        {
            let dir_id_by_path_clone = dir_id_by_path.clone();
            s.spawn(move |_| -> Result<()> {
                let tx = conn.transaction()?;
                {
                    let mut insert_new_node = tx.insert_node_prepare()?;
                    //let mut insert_dir = tx.insert_dir_prepare()?;
                    let mut find_node = tx.fetch_node_prepare()?;
                    let mut update_mtime = tx.update_mtime_prepare()?;
                    loop {
                        match nodereceiver.try_recv() {
                            Ok(node) => {
                                if node.get_type() == &Dir {
                                    let id: i64 = find_upsert_node(
                                        &mut insert_new_node,
                                        &mut find_node,
                                        &mut update_mtime,
                                        &node,
                                    )?
                                        .get_id();
                                    let p: PathBuf = PathBuf::from(node.get_name());
                                    //println!(" dir id for path:{:?} is {}", p.as_path(), id.clone());
                                    //dir_id_by_path.insert(p, id);
                                    //diridsender.send((p, id))?;
                                    if let Some(mut dirid_writer) = dir_id_by_path_clone.write().ok() {
                                        dirid_writer.insert(p, id);
                                    }
                                    //present.insert(id);
                                } else {
                                    let _id = find_upsert_node(
                                        &mut insert_new_node,
                                        &mut find_node,
                                        &mut update_mtime,
                                        &node,
                                    )?
                                        .get_id();
                                    //present.insert(id);
                                }
                            }
                            Err(TryRecvError::Disconnected) => {
                                break },
                            Err(_) => {}
                        }
                        yield_now()
                    }
                }
                //drop(diridsender);
                tx.commit()?;
                Ok(())
            });
        }
        {
            WalkDir::new(root)
                .follow_links(true)
                .skip_hidden(true)
                .min_depth(1)
                .process_read_dir(move |depth, path, _,  children| {
                    if  depth.is_some()
                    {
                        let payload = PayLoad::new( depth.unwrap(),
                            Arc::new(path.to_path_buf()),
                            children,
                        );
                        payloadsender.send(payload).unwrap();
                    }
                })
                .into_iter()
                .for_each(|_| {

                });
        }

    })
    .expect("failed to spawn thread for dir insertion");
    Ok(())
}

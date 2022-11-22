extern crate bimap;
extern crate clap;
extern crate crossbeam;
extern crate num;
#[macro_use]
extern crate num_derive;

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::env::current_dir;
use std::ffi::{OsStr, OsString};
use std::fs::{FileType, Metadata};
use std::hash::{Hash, Hasher};
use std::io::Error;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::yield_now;
use std::time::{Duration, SystemTime};
use std::vec::Drain;

use anyhow::Result;
use clap::Parser;
use crossbeam::channel::Sender;
use crossbeam::thread;
use rusqlite::{Connection, Row};
use walkdir::DirEntry;
use walkdir::WalkDir;

use db::ForEachClauses;
use db::RowType::{Dir, Grp};
use db::{Node, RowType};

use crate::db::{init_db, is_initialized, LibSqlExec, LibSqlPrepare, SqlStatement};
use crate::parse::{find_upsert_node, parse_tupfiles_in_db};

mod db;
mod parse;
const MAX_THRS_NODES: u8 = 8;
const MAX_THRS_DIRS: u8 = 6;
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

/// `DirE` is our version of Walkdir's DirEntry. Keeping only the information we are interested in
#[derive(Clone, Debug)]
struct DirE {
    file_type: FileType,
    file_name: OsString,
    metadata: Option<Metadata>,
}

impl DirE {
    fn from(de: DirEntry) -> DirE {
        DirE {
            file_type: de.file_type(),
            file_name: de.file_name().to_owned(),
            metadata: de.metadata().ok(),
        }
    }

    fn path(&self, parent: &Path) -> PathBuf {
        parent.join(&self.file_name)
    }

    fn file_type(&self) -> &FileType {
        &self.file_type
    }

    fn file_name(&self) -> &OsStr {
        self.file_name.as_os_str()
    }

    fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }
}

#[derive(Debug)]
struct Payload {
    parent_path: Arc<PathBuf>,
    children: Vec<DirE>,
}
#[derive(Debug)]
struct PayloadDir {
    parent_path: Arc<PathBuf>,
    sender: Sender<PayloadDir>,
}

impl Eq for Payload {}
impl Hash for Payload {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parent_path.hash(state)
    }
}
impl Payload {
    pub fn new(parent_path: Arc<PathBuf>, children: Vec<DirE>) -> Payload {
        Payload {
            parent_path,
            children,
        }
    }

    fn parent_path(&self) -> &Path {
        self.parent_path.deref().as_path()
    }
    fn get_children(&mut self) -> Drain<'_, DirE> {
        self.children.drain(..)
    }
}

impl PayloadDir {
    pub fn new(parent_path: Arc<PathBuf>, sender: Sender<PayloadDir>) -> PayloadDir {
        PayloadDir {
            parent_path,
            sender,
        }
    }

    fn parent_path(&self) -> Arc<PathBuf> {
        self.parent_path.clone()
    }
    fn sender(&self) -> &Sender<PayloadDir> {
        &self.sender
    }
}
#[derive(Clone, Debug)]
struct ProtoNode {
    p: DirE,
    pid: i64,
    pbuf: Arc<PathBuf>,
}

impl PartialEq<Self> for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.parent_path.deref() == (other.parent_path.deref())
    }
}

impl PartialOrd for Payload {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.parent_path().partial_cmp(other.parent_path())
    }
}

///---------
impl PartialEq<Self> for PayloadDir {
    fn eq(&self, other: &Self) -> bool {
        self.parent_path.deref() == (other.parent_path.deref())
    }
}

impl Borrow<Path> for Payload {
    fn borrow(&self) -> &Path {
        self.parent_path()
    }
}

fn walkdir_from(root: Arc<PathBuf>, hs: &Sender<PayloadDir>, ps: Sender<Payload>) {
    let mut children = Vec::new();
    WalkDir::new(root.deref())
        .follow_links(true)
        //.skip_hidden(true)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .for_each(|e: DirEntry| {
            if e.file_type().is_dir() {
                let pdir = PayloadDir::new(Arc::new(e.path().to_path_buf()), hs.clone());
                hs.send(pdir).unwrap();
            }
            children.push(DirE::from(e))
        });
    let payload = Payload::new(root, children);
    ps.send(payload).unwrap();
}

fn process_dir(mut payload: Payload, pid: i64, ds: &Sender<ProtoNode>) -> Result<()> {
    let pp = payload.parent_path.clone();
    for p in payload.get_children() {
        ds.send(ProtoNode {
            p,
            pid,
            pbuf: pp.clone(),
        })?;
    }
    Ok(())
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

    //let (diridsender, dirid_receiver) = crossbeam::channel::unbounded::<(PathBuf, i64)>();
    thread::scope(|s| {
        let mut payloadset = HashSet::new();
        let mut dir_id_by_path = HashMap::new();
        let (payloadsender, payloadreceiver) = crossbeam::channel::unbounded();
        let (diridsender, diridreceiver) = crossbeam::channel::bounded::<(PathBuf, i64)>(100);
        dir_id_by_path.insert(root.to_path_buf(), 1);
        println!("root:{:?}", root.to_path_buf());
        let done_sending_payloads = Arc::new(std::sync::atomic::AtomicU8::new(0));
        {
            let done_sending_payloads = done_sending_payloads.clone();
            s.spawn(move |_| -> Result<()> {
                let mut remove_keys = Vec::new();
                let mut skip_payloads = false;
                loop {
                    remove_keys.clear();
                    let mut sel = crossbeam::channel::Select::new();
                    let index0 = sel.recv(&diridreceiver);
                    let index1 = if skip_payloads {
                        usize::MAX
                    } else {
                        sel.recv(&payloadreceiver)
                    };
                    while let Ok(oper) = sel.try_select() {
                        if oper.index() == index0 {
                            if let Err(_x) = oper
                                .recv(&diridreceiver)
                                .map(|(pbuf, id)| dir_id_by_path.insert(pbuf, id))
                            {
                                // eprintln!("Error in receiving ids {:?}", x);
                                break;
                            }
                        } else if oper.index() == index1 {
                            if let Err(_x) =
                                oper.recv(&payloadreceiver).map(|p| payloadset.insert(p))
                            {
                                skip_payloads = true;
                                break;
                            }
                        } else {
                            eprintln!("unknown index returned in select");
                            break;
                        }
                    }
                    if !payloadset.is_empty() || !dir_id_by_path.is_empty() {
                        linkup_dbids(
                            &dire_sender,
                            &mut payloadset,
                            &mut dir_id_by_path,
                            &mut remove_keys,
                        )?;
                    } else if done_sending_payloads.load(Ordering::SeqCst) == MAX_THRS_DIRS {
                        break;
                    }
                    yield_now();
                }
                Ok(())
            });
        }
        {
            // threads below convert DirEntries to Node's ready for sql queries/inserts
            for _ in 0..MAX_THRS_NODES {
                let ns = nodesender.clone();
                let dire_receiver = dire_receiver.clone();
                s.spawn(move |_| -> Result<()> {
                    for p in dire_receiver.iter() {
                        let f = p.p;
                        let pid = p.pid;
                        if f.file_type().is_file() {
                            let fpath = f.path(p.pbuf.as_ref());
                            let metadata = f.metadata();
                            if metadata.is_none() {
                                eprintln!("cannot read metadata for: {:?}", fpath.as_path());
                                continue;
                            }
                            if let Ok(mtime) = time_since_unix_epoch(metadata.unwrap()) {
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
                    }
                    drop(ns);
                    Ok(())
                });
            }
            drop(nodesender);
        }
        {
            // the thread below works with sqlite db to insert or upsert nodes
            s.spawn(move |_| -> Result<()> {
                let tx = conn.transaction()?;
                {
                    let mut insert_new_node = tx.insert_node_prepare()?;
                    //let mut insert_dir = tx.insert_dir_prepare()?;
                    let mut find_node = tx.fetch_node_prepare()?;
                    let mut update_mtime = tx.update_mtime_prepare()?;
                    for node in nodereceiver.iter() {
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
                            diridsender.send((p, id))?;
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
                    println!("node receiver exited");
                }
                tx.commit()?;
                Ok(())
            });
        }
        let (heapsender, heapreceiver) = crossbeam::channel::unbounded();
        heapsender
            .send(PayloadDir::new(
                Arc::new(root.to_path_buf()),
                heapsender.clone(),
            ))
            .unwrap();
        drop(heapsender);
        for _ in 0..MAX_THRS_DIRS {
            let hr = heapreceiver.clone();
            let ps = payloadsender.clone();
            let dc = done_sending_payloads.clone();
            {
                s.spawn(move |_| {
                    for payloaddir in hr.iter() {
                        walkdir_from(payloaddir.parent_path(), payloaddir.sender(), ps.clone());
                    }
                    drop(hr);
                    //println!("done");
                    dc.fetch_add(1, Ordering::SeqCst);
                });
            }
        }
    })
    .expect("failed to spawn thread for dir insertion");
    Ok(())
}

fn linkup_dbids(
    dire_sender: &Sender<ProtoNode>,
    payloadset: &mut HashSet<Payload>,
    dir_id_by_path: &mut HashMap<PathBuf, i64>,
    remove_keys: &mut Vec<PathBuf>,
) -> Result<()> {
    for (parent_path, pid) in dir_id_by_path.iter() {
        //let mut len = 0;
        if let Some(payload) = payloadset.take(parent_path.as_path()) {
            //println!(" -hx {:?} ", len-1);
            remove_keys.push(parent_path.clone());
            process_dir(payload, *pid, &dire_sender)?;
        } else {
            //    println!("mishit:{:?}", parent_path);
        }
    }
    remove_keys.drain(..).for_each(|p| {
        let _ = dir_id_by_path.remove(p.as_path());
    });
    Ok(())
}

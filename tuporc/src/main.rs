extern crate bimap;
extern crate clap;
extern crate crossbeam;
extern crate num;
#[macro_use]
extern crate num_derive;

use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::env::current_dir;
use std::ffi::{OsStr, OsString};
use std::fs::{FileType, Metadata};
use std::hash::BuildHasher;
use std::hash::{Hash, Hasher};
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::yield_now;
use std::time::{Duration, SystemTime};
use std::vec::Drain;

use anyhow::Result;
use clap::Parser;
use crossbeam::channel::Sender;
use crossbeam::sync::WaitGroup;
use crossbeam::thread;
use rusqlite::{Connection, Row};
use walkdir::DirEntry;
use walkdir::WalkDir;

use db::ForEachClauses;
use db::RowType::{Dir, Grp};
use db::{Node, RowType};

use crate::db::{init_db, is_initialized, LibSqlExec, LibSqlPrepare, MiscStatements, SqlStatement};
use crate::parse::{find_upsert_node, parse_tupfiles_in_db};

mod db;
mod parse;
const MAX_THRS_NODES: u8 = 4;
const MAX_THRS_DIRS: u8 = 10;
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
                match scan_root(root.as_path(), &mut conn) {
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
                scan_root(root.as_path(), &mut conn)?;
                parse_tupfiles_in_db(&mut conn, root.as_path())?;
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
fn scan_root(root: &Path, conn: &mut Connection) -> Result<()> {
    insert_direntries(root, conn)
}

// WIP... delete files and rules in db that arent in the filesystem or in use
// should restrict attention to the outputs of tupfiles that are modified/deleted.
fn __delete_missing(conn: &Connection, present: &HashSet<i64>) -> Result<()> {
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
    hashed_path: HashedPath,
}

impl DirE {
    fn new(de: DirEntry, hashed_path: HashedPath) -> DirE {
        DirE {
            file_type: de.file_type(),
            file_name: de.file_name().to_owned(),
            metadata: de.metadata().ok(),
            hashed_path,
        }
    }

    fn get_path(&self) -> &HashedPath {
        &self.hashed_path
    }

    fn file_type(&self) -> &FileType {
        &self.file_type
    }

    fn file_name(&self) -> &OsString {
        &self.file_name
    }

    fn metadata(&self) -> Option<&Metadata> {
        self.metadata.as_ref()
    }
}
#[derive(Debug, Clone)]
struct HashedPath {
    path: Arc<PathBuf>,
    hash: u64,
}

impl Hash for HashedPath {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.hash);
    }
}

impl From<PathBuf> for HashedPath {
    fn from(p: PathBuf) -> Self {
        let rs = RandomState::new();
        let mut hasher = rs.build_hasher();
        p.hash(&mut hasher);
        HashedPath {
            path: Arc::new(p),
            hash: hasher.finish(),
        }
    }
}

impl AsRef<Path> for HashedPath {
    fn as_ref(&self) -> &Path {
        self.path.as_path()
    }
}

impl HashedPath {
    fn get_hash(&self) -> u64 {
        self.hash
    }
    fn get_path(&self) -> Arc<PathBuf> {
        self.path.clone()
    }
}

#[derive(Debug)]
struct Payload {
    parent_path: HashedPath,
    children: Vec<DirE>,
}
#[derive(Debug)]
struct PayloadDir {
    parent_path: HashedPath,
    sender: Sender<PayloadDir>,
}

impl Eq for Payload {}
impl Hash for Payload {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parent_path.hash(state)
    }
}

impl Payload {
    pub fn new(parent_path: HashedPath, children: Vec<DirE>) -> Payload {
        Payload {
            parent_path,
            children,
        }
    }

    fn parent_path(&self) -> &HashedPath {
        &self.parent_path
    }
    fn get_children(&mut self) -> Drain<'_, DirE> {
        self.children.drain(..)
    }
}

impl PayloadDir {
    pub fn new(parent_path: HashedPath, sender: Sender<PayloadDir>) -> PayloadDir {
        PayloadDir {
            parent_path,
            sender,
        }
    }

    fn parent_path(&self) -> HashedPath {
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
}

impl ProtoNode {
    pub fn new(p: DirE, pid: i64) -> ProtoNode {
        ProtoNode { p, pid }
    }

    fn get_dire(&self) -> &DirE {
        &self.p
    }

    fn get_file_name(&self) -> &OsStr {
        &self.p.file_name
    }

    fn get_parent_id(&self) -> i64 {
        self.pid
    }

    fn get_path(&self) -> &HashedPath {
        &self.p.get_path()
    }
}
#[derive(Clone, Debug)]
struct NodeAtPath {
    node: Node,
    pbuf: HashedPath,
}

impl NodeAtPath {
    pub fn new(node: Node, pbuf: HashedPath) -> NodeAtPath {
        NodeAtPath { node, pbuf }
    }

    pub fn get_node(&self) -> &Node {
        &self.node
    }
    pub fn get_hashed_path(&self) -> &HashedPath {
        &self.pbuf
    }
}

impl PartialEq<Self> for HashedPath {
    fn eq(&self, other: &Self) -> bool {
        self.get_hash() == other.get_hash()
            && self.get_path().as_path() == other.get_path().as_path()
    }
}
impl Eq for HashedPath {}

impl PartialEq<Self> for Payload {
    fn eq(&self, other: &Self) -> bool {
        self.parent_path() == other.parent_path()
    }
}

impl PartialEq<Self> for PayloadDir {
    fn eq(&self, other: &Self) -> bool {
        self.parent_path() == other.parent_path()
    }
}

impl Borrow<HashedPath> for Payload {
    fn borrow(&self) -> &HashedPath {
        self.parent_path()
    }
}

fn walkdir_from(root: HashedPath, hs: &Sender<PayloadDir>, ps: Sender<Payload>) {
    let mut children = Vec::new();
    WalkDir::new(root.as_ref())
        .follow_links(true)
        //.skip_hidden(true)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(|e| e.ok())
        .for_each(|e: DirEntry| {
            let pp = HashedPath::from(e.path().to_path_buf());
            let cur_path = pp.clone();
            let pdir = PayloadDir::new(pp, hs.clone());
            if e.file_type().is_dir() {
                hs.send(pdir).unwrap();
            }
            children.push(DirE::new(e, cur_path))
        });
    let payload = Payload::new(root, children);
    ps.send(payload).unwrap();
}

fn send_children(mut payload: Payload, pid: i64, ds: &Sender<ProtoNode>) -> Result<()> {
    for p in payload.get_children() {
        ds.send(ProtoNode::new(p, pid))?;
    }
    Ok(())
}
/// insert directory entries into Node table if not already added.
fn insert_direntries(root: &Path, conn: &mut Connection) -> Result<()> {
    db::create_present_temptable(conn)?;
    println!("Sqlite version: {}\n", rusqlite::version());
    {
        let existing_node = conn.fetch_node_prepare()?.fetch_node(".", 0).ok();
        let n = existing_node.map(|n| n.get_id());
        let n = if n.is_none() {
            let mut insert_dir = conn.insert_dir_prepare()?;
            let id = insert_dir.insert_dir_exec(".", 0)?;
            id
        }else {
            n.unwrap()
        };
        anyhow::ensure!(n == 1, format!("unexpected id for root dir :{} ", n));
        conn.insert_present_prepare()?.insert_present(n)?;
    }

    thread::scope(|s| -> Result<()> {
        let (nodesender, nodereceiver) = crossbeam::channel::unbounded();
        let (dire_sender, dire_receiver) = crossbeam::channel::unbounded::<ProtoNode>();
        let (send_done, recv_done) = crossbeam::channel::bounded(0);
        let mut payload_set = HashSet::new();
        let mut dir_id_by_path = HashMap::new();
        let (payload_sender, payload_receiver) = crossbeam::channel::unbounded();
        let (dirid_sender, dirid_receiver) = crossbeam::channel::bounded::<(HashedPath, i64)>(100);
        let root_hp = HashedPath::from(root.to_path_buf());
        let root_hash_path = root_hp.clone();
        dir_id_by_path.insert(root_hp, 1);
        println!("root:{:?}", root.to_path_buf());
        {
            s.spawn(move |_| -> Result<()> {
                let mut skip_payloads = false;
                let mut end_payload_work = false;
                loop {
                    let mut sel = crossbeam::channel::Select::new();
                    let index0 = sel.recv(&dirid_receiver);
                    let index1 = if skip_payloads {
                        usize::MAX
                    } else {
                        sel.recv(&payload_receiver)
                    };
                    let index2 = if end_payload_work {
                        usize::MAX
                    } else {
                        sel.recv(&recv_done)
                    };

                    while let Ok(oper) = sel.try_select() {
                        if oper.index() == index0 {
                            if oper
                                .recv(&dirid_receiver)
                                .map(|(p, id)| dir_id_by_path.insert(p, id))
                                .is_err()
                            {
                                // eprintln!("Error in receiving ids {:?}", x);
                                break;
                            }
                        } else if oper.index() == index1 {
                            if oper
                                .recv(&payload_receiver)
                                .map(|p| payload_set.insert(p))
                                .is_err()
                            {
                                skip_payloads = true;
                                break;
                            }
                        } else if oper.index() == index2 {
                            if oper.recv(&recv_done).is_ok() {
                                end_payload_work = true;
                            }
                        } else {
                            eprintln!("unknown index returned in select");
                            break;
                        }
                    }
                    if !payload_set.is_empty() || !dir_id_by_path.is_empty() {
                        linkup_dbids(
                            &dire_sender,
                            &mut payload_set,
                            &mut dir_id_by_path,
                        )?;
                    } else if end_payload_work {
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
                        let f = p.get_dire();
                        let pid = p.get_parent_id();
                        let file_name = p.get_file_name();
                        if f.file_type().is_file() {
                            let metadata = f.metadata();
                            if metadata.is_none() {
                                eprintln!("cannot read metadata for: {:?}", file_name);
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
                                    file_name.to_string_lossy().to_string(),
                                    rtype,
                                );
                                ns.send(NodeAtPath::new(node, p.get_path().clone()))?;
                            }
                        } else if f.file_type().is_dir() {
                            let node =
                                Node::new(0, pid, 0, file_name.to_string_lossy().to_string(), Dir);
                            ns.send(NodeAtPath::new(node, p.get_path().clone()))?;
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
                    let mut find_node = tx.fetch_node_prepare()?;
                    let mut insert_present = tx.insert_present_prepare()?;
                    let mut update_mtime = tx.update_mtime_prepare()?;
                    let mut insert_modify = tx.insert_modify_prepare()?;

                    for tnode in nodereceiver.iter() {
                        let node = tnode.get_node();
                        if node.get_type() == &Dir {
                            let id: i64 = find_upsert_node(
                                &mut insert_new_node,
                                &mut find_node,
                                &mut update_mtime,
                                &mut insert_present,
                                &mut insert_modify,
                                &node,
                            )?
                            .get_id();

                            dirid_sender.send((tnode.get_hashed_path().clone(), id))?;
                        } else {
                            find_upsert_node(
                                &mut insert_new_node,
                                &mut find_node,
                                &mut update_mtime,
                                &mut insert_present,
                                &mut insert_modify,
                                &node,
                            )?;
                        }
                    }
                }
                tx.commit()?;
                conn.populate_delete_list()?;
                Ok(())
            });
        }
        let (dirpayload_sender, dirpayload_receiver) = crossbeam::channel::unbounded();
        let pl = PayloadDir::new(root_hash_path, dirpayload_sender.clone());
        dirpayload_sender.send(pl)?;
        drop(dirpayload_sender);
        let wg = WaitGroup::new();
        for _ in 0..MAX_THRS_DIRS {
            let hr = dirpayload_receiver.clone();
            let ps = payload_sender.clone();
            {
                let wg = wg.clone();
                s.spawn(move |_| {
                    for payloaddir in hr.iter() {
                        walkdir_from(payloaddir.parent_path(), payloaddir.sender(), ps.clone());
                    }
                    drop(hr);
                    drop(wg);
                });
            }
        }
        wg.wait();
        send_done.send(())?;
        Ok(())
    })
    .expect("Failed to spawn thread for dir insertion")
    .expect("Failed to return from scope");
    Ok(())
}

/// This tracks the paths which have a available parent id, removes them from payloads, and dir_id_by_path and sends them over to write/query to/from db
fn linkup_dbids(
    dire_sender: &Sender<ProtoNode>,
    payload_set: &mut HashSet<Payload>,
    dir_id_by_path: &mut HashMap<HashedPath, i64>,
) -> Result<()> {

    dir_id_by_path.
        retain(|p, id|
            // warning this is a predicate with side effects.. :-(
        if let Some(payload) = payload_set.take(p) {
            send_children(payload, *id, &dire_sender).unwrap_or_else(|_| panic!("unable to send directory:{:?}", p));
            false
        } else {
            true
        }
    );
    Ok(())
}

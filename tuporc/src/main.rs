extern crate bimap;
extern crate clap;
extern crate crossbeam;
extern crate env_logger;
extern crate execute;

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::RandomState;
use std::env::current_dir;
use std::ffi::{OsStr, OsString};
use std::fs::{FileType, Metadata};
use std::hash::{Hash, Hasher};
use std::hash::BuildHasher;
use std::io::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::yield_now;
use std::time::{Duration, SystemTime};
use std::vec::Drain;

use anyhow::Result;
use clap::Parser;
use crossbeam::channel::{Receiver, Sender};
use crossbeam::sync::WaitGroup;
use crossbeam::thread;
use rusqlite::{Connection, Row};
use walkdir::DirEntry;
use walkdir::WalkDir;

use db::{Node, RowType};
//use db::ForEachClauses;
use db::RowType::{Dir, Grp};

use crate::db::{init_db, is_initialized, LibSqlExec, LibSqlPrepare, MiscStatements, SqlStatement};
use crate::parse::{find_upsert_node, gather_tupfiles, parse_tupfiles_in_db};

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
    let rtype: i8 = row.get(2)?;
    let name: String = row.get(3)?;
    let mtime: i64 = row.get(4)?;
    let rtype = match rtype {
        0 => RowType::File,
        1 => RowType::Rule,
        2 => Dir,
        3 => RowType::Env,
        4 => RowType::GenF,
        5 => RowType::TupF,
        6 => Grp,
        7 => RowType::GenD,
        _ => panic!("Invalid type {} for row with id:{}", rtype, id),
    };
    Ok(Node::new(id, pid, mtime, name, rtype))
}
fn make_rule_node(row: &Row) -> rusqlite::Result<Node> {
    let id: i64 = row.get(0)?;
    let pid: i64 = row.get(1)?;
    let name: String = row.get(2)?;
    let display_str: String = row.get(3)?;
    let flags: String = row.get(4)?;

    Ok(Node::new_rule(id, pid, name, display_str, flags))
}

fn main() -> Result<()> {
    let args = Args::parse();
    env_logger::init();
    log::info!("Sqlite version: {}\n", rusqlite::version());
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
                let root = current_dir()?;
                let tupfiles = {
                    let mut conn = Connection::open(".tup/db")
                        .expect("Connection to tup database in .tup/db could not be established");
                    if !is_initialized(&conn) {
                        return Err(anyhow::Error::msg(
                            "Tup database is not initialized, use `tup init' to initialize",
                        ));
                    }
                    println!("Parsing tupfiles in database");
                    scan_root(root.as_path(), &mut conn)?;
                    gather_tupfiles(&mut conn)?
                };
                parse_tupfiles_in_db(tupfiles, root.as_path(), false)?;
            }
            Action::Upd { target } => {
                println!("Updating db {}", target.join(" "));
                let root = current_dir()?;
                let tupfiles = {
                    let mut conn = Connection::open(".tup/db")
                        .expect("Connection to tup database in .tup/db could not be established");
                    if !is_initialized(&conn) {
                        return Err(anyhow::Error::msg(
                            "Tup database is not initialized, use `tup init' to initialize",
                        ));
                    }
                    println!("Parsing tupfiles in database");
                    scan_root(root.as_path(), &mut conn)?;
                    gather_tupfiles(&mut conn)?
                };
                parse_tupfiles_in_db(tupfiles, root.as_path(), true)?;
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
fn __delete_missing(_conn: &Connection, _present: &HashSet<i64>) -> Result<()> {
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
struct DirChildren {
    parent_path: HashedPath,
    children: Vec<DirE>,
}
#[derive(Debug)]
struct DirSender {
    parent_path: HashedPath,
    sender: Sender<DirSender>,
}

impl Eq for DirChildren {}
impl Hash for DirChildren {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.parent_path.hash(state)
    }
}

impl DirChildren {
    pub fn new(parent_path: HashedPath, children: Vec<DirE>) -> DirChildren {
        DirChildren {
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

impl DirSender {
    pub fn new(parent_path: HashedPath, sender: Sender<DirSender>) -> DirSender {
        DirSender {
            parent_path,
            sender,
        }
    }

    fn parent_path(&self) -> HashedPath {
        self.parent_path.clone()
    }
    fn sender(&self) -> &Sender<DirSender> {
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
        self.p.get_path()
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

impl PartialEq<Self> for DirChildren {
    fn eq(&self, other: &Self) -> bool {
        self.parent_path() == other.parent_path()
    }
}

impl PartialEq<Self> for DirSender {
    fn eq(&self, other: &Self) -> bool {
        self.parent_path() == other.parent_path()
    }
}

impl Borrow<HashedPath> for DirChildren {
    fn borrow(&self) -> &HashedPath {
        self.parent_path()
    }
}

fn walkdir_from(root: HashedPath, hs: &Sender<DirSender>, ps: Sender<DirChildren>) -> Result<()> {
    let mut children = Vec::new();
    for e in WalkDir::new(root.as_ref())
        .follow_links(true)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
    {
        let pp = HashedPath::from(e.path().to_path_buf());
        let cur_path = pp.clone();
        let pdir = DirSender::new(pp, hs.clone());
        if e.file_type().is_dir() {
            hs.send(pdir)?;
        }
        children.push(DirE::new(e, cur_path))
    }
    ps.send(DirChildren::new(root, children))?;
    Ok(())
}

fn send_children(mut dir_children: DirChildren, pid: i64, ds: &Sender<ProtoNode>) -> Result<()> {
    for p in dir_children.get_children() {
        ds.send(ProtoNode::new(p, pid))?;
    }
    Ok(())
}
/// insert directory entries into Node table if not already added.
fn insert_direntries(root: &Path, conn: &mut Connection) -> Result<()> {
    log::debug!("Inserting/updating directory entries to db");
    db::create_temptables(conn)?;
    {
        let n = conn.fetch_nodeid_prepare()?.fetch_node_id(".", 0).ok();
        let n = n.ok_or_else(|| anyhow::Error::msg("no such node : '.'"));
        let n = n.or_else(|_| -> Result<i64> {
            let mut insert_dir = conn.insert_dir_prepare()?;
            let i = insert_dir.insert_dir_exec(".", 0)?;
            Ok(i)
        })?;
        anyhow::ensure!(n == 1, format!("unexpected id for root dir :{} ", n));
        conn.add_to_present_prepare()?.add_to_present_exec(n, Dir)?;
    }

    // Begin processessing
    thread::scope(|s| -> Result<()> {
        let (nodesender, nodereceiver) = crossbeam::channel::unbounded();
        let (dire_sender, dire_receiver) = crossbeam::channel::unbounded::<ProtoNode>();
        // channel for communicating that we are done with finding ids of directories.
        //let (send_dirs_done, recv_dirs_done) = crossbeam::channel::bounded(0);
        // channel for communicating  that we are done with finding children of directories.
        //let (send_done, recv_done) = crossbeam::channel::bounded(0);
        // Map of children of  directories as returned by walkdir. `DirChildren`
        let mut dir_children_set = HashSet::new();
        // map of database ids of directories
        let mut dir_id_by_path = HashMap::new();
        // channel for  tracking children of a directory `DirChildren`.
        let (dir_children_sender, dir_children_receiver) = crossbeam::channel::unbounded();
        let (dirid_sender, dirid_receiver) = crossbeam::channel::unbounded();
        let root_hp = HashedPath::from(root.to_path_buf());
        let root_hash_path = root_hp.clone();
        dir_id_by_path.insert(root_hp, 1);
        println!("root:{:?}", root.to_path_buf());
        {
            s.spawn(move |_| -> Result<()> {
                let mut end_dirs = false;
                let mut end_dir_children = false;
                loop {
                    let mut sel = crossbeam::channel::Select::new();
                    let index_dir_receiver = if end_dirs {
                        usize::MAX
                    } else {
                        sel.recv(&dirid_receiver)
                    };

                    let index_dir_children_recv = if end_dir_children {
                        usize::MAX
                    } else {
                        sel.recv(&dir_children_receiver)
                    };
                    let mut changed = false;
                    while let Ok(oper) = sel.try_select() {
                        if oper.index() == index_dir_receiver {
                            oper.recv(&dirid_receiver).map_or_else(
                                |_| {
                                    log::debug!("no more dirs  expected");
                                    end_dirs = true;
                                },
                                |(p, id)| {
                                    dir_id_by_path.insert(p, id);
                                    changed = true;
                                },
                            );
                        } else if oper.index() == index_dir_children_recv {
                            oper.recv(&dir_children_receiver).map_or_else(
                                |_| {
                                    log::debug!("no more dir children expected");
                                    end_dir_children = true;
                                },
                                |p| {
                                    dir_children_set.insert(p);
                                    changed = true;
                                },
                            );
                        } else {
                            eprintln!("unknown index returned in select");
                            break;
                        }
                        if end_dirs || end_dir_children {
                            break;
                        }
                    }
                    if !dir_children_set.is_empty() || !dir_id_by_path.is_empty() {
                        if changed {
                            linkup_dbids(&dire_sender, &mut dir_children_set, &mut dir_id_by_path)?;
                        }
                    } else if end_dir_children {
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
            // hidden in a corner is this thread below that works with sqlite db to upsert nodes
            // Nodes are expected to have parent dir ids.
            // Once a dir is upserted this also sends database ids of dirs so that new  children of those dirs can be inserted with parent dir id.
            s.spawn(move |_| -> Result<()> {
                let res = add_modify_nodes(conn, nodereceiver, dirid_sender);
                log::debug!("finished adding nodes");
                if res.is_err() {
                    log::debug!("with Err:{:?}", res)
                }
                res
            });
        }
        let (dirs_sender, dirs_receiver) = crossbeam::channel::unbounded();
        let pl = DirSender::new(root_hash_path, dirs_sender.clone());
        dirs_sender.send(pl)?;
        drop(dirs_sender);
        let wg = WaitGroup::new();
        for _ in 0..MAX_THRS_DIRS {
            // This loop spreads the task for walking over children among threads.
            // walkdir is only run for immediate children. When  we encounder dirs, they are packaged in `dir_sender` thereby queuing them until
            // they are popped for running walkdir on them.
            let hr = dirs_receiver.clone();
            let ps = dir_children_sender.clone();
            let wg = wg.clone();
            s.spawn(move |_| -> Result<()> {
                let res = walk_recvd_dirs(hr, ps);
                drop(wg);
                res
            });
        }
        wg.wait();
        Ok(())
    })
    .expect("Failed to spawn thread for dir insertion")
    .expect("Failed to return from scope");
    Ok(())
}

// dir with db ids are available now walk over their children (`DirChildren` and send them for upserts
fn walk_recvd_dirs(hr: Receiver<DirSender>, ps: Sender<DirChildren>) -> Result<()> {
    for dir_sender in hr.iter() {
        walkdir_from(dir_sender.parent_path(), dir_sender.sender(), ps.clone())?;
    }
    Ok(())
}

/// For the received nodes which are are already in database (uniqueness of name, dir), this will attempt to update their timestamps.
/// Those not in database will be inserted.
fn add_modify_nodes(
    conn: &mut Connection,
    nodereceiver: Receiver<NodeAtPath>,
    dirid_sender: Sender<(HashedPath, i64)>,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut insert_new_node = tx.insert_node_prepare()?;
        let mut find_node = tx.fetch_node_prepare()?;
        let mut add_to_present = tx.add_to_present_prepare()?;
        let mut update_mtime = tx.update_mtime_prepare()?;
        let mut add_to_modify = tx.add_to_modify_prepare()?;

        for tnode in nodereceiver.iter() {
            let node = tnode.get_node();
            log::debug!("recvd {}, {:?}", node.get_name(), node.get_type());
            if node.get_type() == &Dir {
                let id: i64 = find_upsert_node(
                    &mut insert_new_node,
                    &mut find_node,
                    &mut update_mtime,
                    &mut add_to_present,
                    &mut add_to_modify,
                    node,
                )?
                .get_id();

                dirid_sender.send((tnode.get_hashed_path().clone(), id))?;
            } else {
                find_upsert_node(
                    &mut insert_new_node,
                    &mut find_node,
                    &mut update_mtime,
                    &mut add_to_present,
                    &mut add_to_modify,
                    node,
                )?;
            }
        }
    }
    tx.commit()?;
    conn.populate_delete_list()?;

    Ok(())
}

/// This tracks the paths which have a available parent id, removes them from dir_children, and dir_id_by_path and sends them over to write/query to/from db
fn linkup_dbids(
    dire_sender: &Sender<ProtoNode>,
    dir_children_set: &mut HashSet<DirChildren>,
    dir_id_by_path: &mut HashMap<HashedPath, i64>,
) -> Result<()> {
    // retains only dir ids for which its children were not found in `dir_children`.
    dir_id_by_path.
        retain(|p, id|
            // warning this is a predicate with side effects.. :-(.
        if let Some(dir_children) = dir_children_set.take(p) {
            send_children(dir_children, *id, dire_sender).unwrap_or_else(|_| panic!("unable to send directory:{:?}", p));
            false
        } else {
            true
        }
    );
    Ok(())
}

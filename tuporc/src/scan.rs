use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::{FileType, Metadata};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::Error as IOError;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::yield_now;
use std::time::{Duration, SystemTime};
use std::vec::Drain;

use crossbeam::channel::{Receiver, Sender};
use crossbeam::sync::WaitGroup;
use eyre::eyre;
use indicatif::ProgressBar;
use rusqlite::Connection;
use walkdir::{DirEntry, WalkDir};

use crate::db::RowType::{Dir, TupF};
use crate::db::{LibSqlExec, LibSqlPrepare, MiscStatements, Node, RowType, SqlStatement};
use crate::parse::find_upsert_node;
use crate::{db, parse, TermProgress};

const MAX_THRS_NODES: u8 = 4;
pub const MAX_THRS_DIRS: u8 = 10;

/// handle the tup scan command by walking the directory tree and adding dirs and files into node table.
pub(crate) fn scan_root(
    root: &Path,
    conn: &mut Connection,
    term_progress: &TermProgress,
) -> eyre::Result<()> {
    insert_direntries(root, conn, term_progress)
}

/// return dir id either from db stored value in readstate or from newly created list in created dirs
pub(crate) fn get_dir_id<P: AsRef<Path>>(dirs_in_db: &mut SqlStatement, path: P) -> Option<i64> {
    dirs_in_db.fetch_dirid(path).ok() // check if in db already
}

/// mtime stored wrt 1-1-1970
fn time_since_unix_epoch(meta_data: &Metadata) -> eyre::Result<Duration, IOError> {
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

fn walkdir_from(
    root: HashedPath,
    hs: &Sender<DirSender>,
    ps: Sender<DirChildren>,
) -> eyre::Result<()> {
    let mut children = Vec::new();
    for e in WalkDir::new(root.as_ref())
        .follow_links(true)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(eyre::Result::ok)
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

fn send_children(
    mut dir_children: DirChildren,
    pid: i64,
    ds: &Sender<ProtoNode>,
) -> eyre::Result<()> {
    for p in dir_children.get_children() {
        ds.send(ProtoNode::new(p, pid))?;
    }
    Ok(())
}

/// insert directory entries into Node table if not already added.
fn insert_direntries(
    root: &Path,
    conn: &mut Connection,
    term_progress: &TermProgress,
) -> eyre::Result<()> {
    log::debug!("Inserting/updatng directory entries to db");
    db::create_temptables(conn)?;
    {
        let n = conn.fetch_nodeid_prepare()?.fetch_node_id(".", 0).ok();
        let n = n.ok_or_else(|| eyre!("no such node : '.'"));
        let n = n.or_else(|_| -> eyre::Result<i64> {
            let mut insert_dir = conn.insert_dir_prepare()?;
            let i = insert_dir.insert_dir_exec(".", 0)?;
            Ok(i)
        })?;
        eyre::ensure!(n == 1, format!("unexpected id for root dir :{} ", n));
        conn.add_to_present_prepare()?.add_to_present_exec(n, Dir)?;
    }

    let pb = term_progress.pb_main.clone();
    // Begin processessing
    crossbeam::scope(|s| -> eyre::Result<()> {
        let (nodesender, nodereceiver) = crossbeam::channel::unbounded();
        let (dire_sender, dire_receiver) = crossbeam::channel::unbounded::<ProtoNode>();
        // Map of children of  directories as returned by walkdir. `DirChildren`
        let mut dir_children_set = HashSet::new();
        // map of database ids of directories
        let mut dir_id_by_path = HashMap::new();
        // channel for  tracking children of a directory `DirChildren`.
        let (dir_children_sender, dir_children_receiver) = crossbeam::channel::unbounded();
        // channel for tracking db ids of directories. This is an essential pre-step for inserting files into db.
        let (dirid_sender, dirid_receiver) = crossbeam::channel::unbounded();
        let root_hp = HashedPath::from(root.to_path_buf());
        let root_hash_path = root_hp.clone();
        dir_id_by_path.insert(root_hp, 1);
        //println!("root:{:?}", root.to_path_buf());
        {
            s.spawn(move |_| -> eyre::Result<()> {
                let mut end_dirs = false;
                let mut end_dir_children = false;
                loop {
                    let mut sel = crossbeam::channel::Select::new();
                    let index_dir = if end_dirs {
                        usize::MAX
                    } else {
                        sel.recv(&dirid_receiver)
                    };

                    let index_dir_children = if end_dir_children {
                        usize::MAX
                    } else {
                        sel.recv(&dir_children_receiver)
                    };
                    let mut changed = false;
                    while let Ok(oper) = sel.try_select() {
                        match oper.index() {
                            i if i == index_dir => {
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
                            }
                            i if i == index_dir_children => {
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
                            }
                            _ => {
                                eprintln!("unknown index returned in select");
                                break;
                            }
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
                s.spawn(move |_| -> eyre::Result<()> {
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
                                let rtype = if crate::is_tupfile(f.file_name()) {
                                    TupF
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
            let pb = pb.clone();
            s.spawn(move |_| -> eyre::Result<()> {
                let res = add_modify_nodes(conn, nodereceiver, dirid_sender, &pb);
                log::debug!("finished adding nodes");
                if res.is_err() {
                    log::error!("Error:{:?}", res);
                    term_progress.abandon(&pb, " Errors encountered during scanning.")
                } else {
                    term_progress.finish(&pb, "✔ Finished scanning");
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
            s.spawn(move |_| -> eyre::Result<()> {
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
fn walk_recvd_dirs(hr: Receiver<DirSender>, ps: Sender<DirChildren>) -> eyre::Result<()> {
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
    progressbar: &ProgressBar,
) -> eyre::Result<()> {
    let tx = conn.transaction()?;
    {
        let mut node_statements = parse::NodeStatements::new(tx.deref())?;
        let mut add_ids_statements = parse::AddIdsStatements::new(tx.deref())?;
        for tnode in nodereceiver.iter() {
            let node = tnode.get_node();
            log::debug!("recvd {}, {:?}", node.get_name(), node.get_type());
            if node.get_type() == &Dir {
                let id: i64 =
                    find_upsert_node(&mut node_statements, &mut add_ids_statements, node)?.get_id();

                dirid_sender.send((tnode.get_hashed_path().clone(), id))?;
            } else {
                find_upsert_node(&mut node_statements, &mut add_ids_statements, node)?;
            }
            progressbar.tick();
        }
    }
    tx.populate_delete_list()?;
    tx.enrich_modified_list_with_outside_mods()?;
    tx.enrich_modified_list()?;
    tx.prune_modified_list()?;
    tx.commit()?;

    Ok(())
}

/// This tracks the paths which have a available parent id, removes them from dir_children, and dir_id_by_path and sends them over to write/query to/from db
fn linkup_dbids(
    dire_sender: &Sender<ProtoNode>,
    dir_children_set: &mut HashSet<DirChildren>,
    dir_id_by_path: &mut HashMap<HashedPath, i64>,
) -> eyre::Result<()> {
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

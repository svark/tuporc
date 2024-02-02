use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::Metadata;
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
use ignore::gitignore::{Gitignore, GitignoreBuilder};
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
    file_name: OsString,
    metadata: Option<Metadata>,
    hashed_path: Option<HashedPath>, // this is an option to enable taking ownership of the path
}

impl DirE {
    fn from(de: DirEntry) -> DirE {
        DirE {
            file_name: de.file_name().to_owned(),
            metadata: de.metadata().ok(),
            hashed_path: Some(HashedPath::from(de.into_path())),
        }
    }

    fn take_path(&mut self) -> HashedPath {
        self.hashed_path.take().unwrap()
    }

    fn get_hashed_path(&self) -> HashedPath {
        self.hashed_path.clone().unwrap()
    }

    fn is_dir(&self) -> bool {
        self.metadata.as_ref().map_or(false, |m| m.is_dir())
    }

    fn take_metadata(&mut self) -> Option<Metadata> {
        self.metadata.take()
    }
}

#[derive(Debug, Clone)]
pub struct HashedPath {
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

    fn get_file_name(&self) -> &OsStr {
        &self.p.file_name
    }

    fn get_dir_id(&self) -> i64 {
        self.pid
    }

    fn take_path(&mut self) -> HashedPath {
        self.p.take_path()
    }
    pub(crate) fn take_metadata(&mut self) -> Option<Metadata> {
        self.p.take_metadata()
    }
}

/// Node and it hashed path
#[derive(Clone, Debug)]
pub struct NodeAtPath {
    node: Node,
    pbuf: HashedPath,
}

impl NodeAtPath {
    /// create a new NodeAtPath
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
    dir_sender: &Sender<DirSender>,
    dir_entry_sender: Sender<DirChildren>,
) -> eyre::Result<bool> {
    let mut children = Vec::new();
    let mut sent = false;
    WalkDir::new(root.as_ref())
        .follow_links(true)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .try_for_each(|e| {
            let dir_entry = DirE::from(e);
            if dir_entry.is_dir() {
                let pp = dir_entry.get_hashed_path();
                let pdir = DirSender::new(pp, dir_sender.clone());
                dir_sender.send(pdir)?;
                sent = true;
            }
            children.push(dir_entry);
            eyre::Ok(())
        })?;
    dir_entry_sender.send(DirChildren::new(root, children))?;
    Ok(sent)
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

fn create_node_at_path<S: AsRef<str>>(
    dirid: i64,
    file_name: S,
    p: HashedPath,
    mtime: i64,
    rtype: RowType,
) -> NodeAtPath {
    let node = Node::new(0, dirid, mtime, file_name.as_ref().to_string(), rtype);
    NodeAtPath::new(node, p)
}

// prepare node for insertion into db
pub(crate) fn prepare_node_at_path<S: AsRef<str>>(
    dirid: i64,
    file_name: S,
    p: HashedPath,
    metadata: Option<Metadata>,
) -> eyre::Result<Option<NodeAtPath>> {
    let metadata = match metadata {
        Some(metadata) => metadata,
        None => {
            eprintln!(
                "cannot read metadata for directory entry: {:?}",
                file_name.as_ref()
            );
            return Ok(None);
        }
    };
    let mtime = time_since_unix_epoch(&metadata)
        .ok()
        .unwrap_or(Duration::from_secs(0))
        .subsec_nanos() as i64;
    let rtype = if metadata.is_file() {
        if crate::is_tupfile(file_name.as_ref()) {
            TupF
        } else {
            RowType::File
        }
    } else if metadata.is_dir() {
        Dir
    } else {
        return Ok(None);
    };
    Ok(Some(create_node_at_path(dirid, file_name, p, mtime, rtype)))
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
        let ign = build_ignore(root)?;

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
                                        log::debug!("no more dirs expected");
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
                                unreachable!("unknown index returned in select");
                            }
                        }
                        if end_dirs || end_dir_children {
                            break;
                        }
                    }
                    if !dir_children_set.is_empty() || !dir_id_by_path.is_empty() {
                        if changed {
                            linkup_dbids(
                                &ign,
                                &dire_sender,
                                &mut dir_children_set,
                                &mut dir_id_by_path,
                            )?;
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
            // threads below convert DirEntries to Nodes ready for sql queries/inserts
            for _ in 0..MAX_THRS_NODES {
                let ns = nodesender.clone();
                let dire_receiver = dire_receiver.clone();
                s.spawn(move |_| -> eyre::Result<()> {
                    for mut p in dire_receiver.iter() {
                        let dirid = p.get_dir_id();
                        let hashed_path = p.take_path();
                        let metadata = p.take_metadata();
                        let file_name = p.get_file_name().to_string_lossy();
                        let node_at_path =
                            prepare_node_at_path(dirid, file_name, hashed_path, metadata)?;
                        if let Some(node_at_path) = node_at_path {
                            ns.send(node_at_path)?;
                        }
                    }
                    drop(ns);
                    Ok(())
                });
            }
            drop(nodesender);
        }
        {
            // hidden in a corner is this thread below that works with sqlite db to upsert nodes.
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
        dirs_sender.send(DirSender::new(root_hash_path, dirs_sender.clone()))?;
        drop(dirs_sender);
        let wg = WaitGroup::new();
        let mut more_dirs_left = Arc::new(true);
        for _ in 0..MAX_THRS_DIRS {
            // This loop spreads the task for walking over children among threads.
            // walkdir is only run for immediate children. When  we encounder dirs, they are packaged in `dir_sender` thereby queuing them until
            // they are popped for running walkdir on them.
            let dir_receiver = dirs_receiver.clone();
            let dir_children_sender = dir_children_sender.clone();
            let wg = wg.clone();
            let more_di = more_dirs_left.clone();
            s.spawn(move |_| -> eyre::Result<bool> {
                let mut res = walk_recvd_dirs(dir_receiver, dir_children_sender);
                yield_now();
                if res.unwrap_or(false) {
                    res = walk_recvd_dirs(dir_receiver, dir_children_sender);
                }
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

pub(crate) fn build_ignore(root: &Path) -> eyre::Result<Gitignore> {
    let mut binding = GitignoreBuilder::new(root);
    let builder = binding
        .add_line(None, ".tup/**")?
        .add_line(None, ".git/**")?
        .add_line(None, ".tupignore")?;
    if root.join(".tupignore").is_file() {
        let _ = builder
            .add(".tupignore")
            .ok_or(eyre!("unable to add .tupignore"))?;
    }
    let ign = builder
        .build()
        .map_err(|e| eyre!("unable to build tupignore: {:?}", e))?;
    Ok(ign)
}

// dir with db ids are available now walk over their children (`DirChildren` and send them for upserts
fn walk_recvd_dirs(
    dir_receiver: Receiver<DirSender>,
    dir_child_sender: Sender<DirChildren>,
) -> eyre::Result<bool> {
    let mut sent = false;
    for dir_sender in dir_receiver.iter() {
        if walkdir_from(
            dir_sender.parent_path(),
            dir_sender.sender(),
            dir_child_sender.clone(),
        )? {
            sent = true;
        }
    }
    drop(dir_receiver);
    drop(dir_child_sender);
    Ok(sent)
}

/// For the received nodes which are already in database (uniqueness of name, dir), this will attempt to update their timestamps.
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
        for node_at_path in nodereceiver.iter() {
            let node = node_at_path.get_node();
            log::debug!("recvd {}, {:?}", node.get_name(), node.get_type());
            let inserted = find_upsert_node(&mut node_statements, &mut add_ids_statements, node)?;
            if node.get_type() == &Dir {
                let id = inserted.get_id();
                dirid_sender.send((node_at_path.get_hashed_path().clone(), id))?;
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
    ign: &Gitignore,
    dire_sender: &Sender<ProtoNode>,
    dir_children_set: &mut HashSet<DirChildren>,
    dir_id_by_path: &mut HashMap<HashedPath, i64>,
) -> eyre::Result<()> {
    // retains only dir ids for which its children were not found in `dir_children`.
    dir_id_by_path.
        retain(|p, id|
            // warning this is a predicate with side effects.. :-(.
            if ign.matched(p.as_ref(), true).is_ignore() {
                false
            } else if let Some(dir_children) = dir_children_set.take(p) {
                send_children(dir_children, *id, dire_sender).unwrap_or_else(|_| panic!("unable to send directory:{:?}", p));
                false
            } else {
                true
            }
        );
    Ok(())
}

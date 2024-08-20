use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::Metadata;
use std::hash::{BuildHasher, Hash, Hasher};
use std::io::Error as IOError;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::yield_now;
use std::time::{Duration, SystemTime};

use crossbeam::channel::{never, tick, Receiver, Sender};
use crossbeam::select;
use crossbeam::sync::WaitGroup;
use eyre::eyre;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use indicatif::ProgressBar;
use rusqlite::Connection;
use walkdir::{DirEntry, WalkDir};

use crate::db::RowType::{Dir, TupF};
use crate::db::{LibSqlExec, LibSqlPrepare, MiscStatements, Node, RowType};
use crate::parse::find_upsert_node;
use crate::{db, parse, TermProgress};

const MAX_THRS_NODES: u8 = 4;
pub const MAX_THRS_DIRS: u8 = 10;

/// handle the tup scan command by walking the directory tree and adding dirs and files into node table.
pub(crate) fn scan_root(
    root: &Path,
    conn: &mut Connection,
    term_progress: &TermProgress,
    running: Arc<AtomicBool>,
) -> eyre::Result<()> {
    insert_direntries(root, conn, term_progress, running)
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

    fn get(self) -> (HashedPath, Vec<DirE>) {
        (self.parent_path, self.children)
    }
    fn parent_path(&self) -> &HashedPath {
        &self.parent_path
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
    // directory entry to be inserted into db
    dir_id: i64, // parent dir id
}

impl ProtoNode {
    pub fn new(p: DirE, pid: i64) -> ProtoNode {
        ProtoNode { p, dir_id: pid }
    }

    fn get_file_name(&self) -> &OsStr {
        &self.p.file_name
    }

    fn get_dir_id(&self) -> i64 {
        self.dir_id
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

    pub fn get_prepared_node(&self) -> &Node {
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
    ign: &Gitignore,
) -> eyre::Result<()> {
    let mut children = Vec::new();
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
                if !ign.matched(pp.get_path().as_path(), true).is_ignore() {
                    let pdir = DirSender::new(pp, dir_sender.clone());
                    dir_sender.send(pdir)?;
                }
            }
            children.push(dir_entry);
            eyre::Ok(())
        })
        .inspect_err(|e| {
            log::error!(
                "Walkdir failed on folder:{} due to: {}",
                root.get_path().as_path().display(),
                e
            );
        })?;
    dir_entry_sender
        .send(DirChildren::new(root.clone(), children))
        .inspect_err(|e| {
            log::error!(
                "Failed to send children of root:{} due to {}",
                root.get_path().as_path().display(),
                e
            );
        })?;
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
    running: Arc<AtomicBool>,
) -> eyre::Result<()> {
    log::debug!("Inserting/updating directory entries to db");
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
        let (dir_child_sender, dir_children_receiver) =
            crossbeam::channel::unbounded::<DirChildren>();
        // channel for tracking db ids of directories. This is an essential pre-step for inserting files into db.
        let (dirid_sender, dirid_receiver) = crossbeam::channel::unbounded::<(HashedPath, i64)>();
        let root_hp = HashedPath::from(root.to_path_buf());

        let root_hash_path = root_hp.clone();
        let (stop_sender, _) = crossbeam::channel::bounded::<()>(1);
        // Set up the ctrlc handler
        {
            let stop_sender = stop_sender.clone();
            let running = running.clone();
            ctrlc::try_set_handler(move || {
                let _ = stop_sender.send(());
                running.store(false, Ordering::SeqCst);
            })
                .unwrap_or_else(
                    |e| log::warn!("unable to set ctrlc handler:{}", e)
                );
        }

        dir_id_by_path.insert(root_hp, 1);
        let ign = build_ignore(root)?;

        //println!("root:{:?}", root.to_path_buf());
        {
            let ign = ign.clone();
            s.spawn(move |_| -> eyre::Result<()> {
                let ticker = tick(Duration::from_millis(100));
                let mut dirid_receiver = Some(dirid_receiver);
                let mut dir_children_receiver = Some(dir_children_receiver);
                let n1 = never();
                let n2 = never();
                let mut receiver_cnt = 2;
                let running = running.clone();
                let send_children = move |dir_children: DirChildren, id| {
                    let (pp, children) = dir_children.get();
                    for p in children {
                        dire_sender.send(ProtoNode::new(p, id)).unwrap_or_else(|_| {
                            panic!("unable to send directory entry under:{:?}", pp)
                        })
                    }
                };
                while receiver_cnt != 0 {
                    select! {
                        recv(ticker) -> _ =>  {
                            if !running.load(Ordering::SeqCst) {
                                log::debug!("recvd user interrupt");
                                break;
                            }
                            continue;
                        }
                        recv(dirid_receiver.as_ref().unwrap_or(&n1)) -> dirid_res =>
                            match dirid_res {
                                Ok((p, id)) => {
                                    if ign.matched(p.get_path().as_path(), true).is_ignore() {
                                       log::debug!("ignoring path:{}", p.get_path().as_path().display());
                                    }
                                    else if let Some(dir_children) = dir_children_set.take(&p) {
                                        send_children(dir_children, id)
                                    } else {
                                        dir_id_by_path.insert(p, id);
                                    }
                                }
                                _=> {
                                    dirid_receiver = None;
                                    receiver_cnt -= 1;
                                    log::debug!("dirid_receiver closed");
                                }
                            },
                        recv(dir_children_receiver.as_ref().unwrap_or(&n2)) -> res =>
                            match res {
                                Ok(dir_children) => {
                                   if let Some((_,id)) = dir_id_by_path.remove_entry(dir_children.parent_path()) {
                                       send_children(dir_children, id)
                                    }else {
                                        dir_children_set.insert(dir_children);
                                    }
                                }
                                _ => {
                                    dir_children_receiver = None;
                                    receiver_cnt -= 1;
                                    log::debug!("dir_children_receiver closed");
                                }
                            }
                    }
                    yield_now();

                    if receiver_cnt == 1 && dir_children_set.len() == 0 && dir_id_by_path.len() == 0 {
                        // this is a way to break the chain of dependencies among receivers
                        break;
                    }
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
            // hidden in a corner is this thread below that works to upsert nodes into sqlite db
            // Nodes are expected to have parent dir ids at this stage.
            // Once a dir is upserted this also sends database ids of dirs so that children of inserted dirs can also be inserted
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
        for _ in 0..MAX_THRS_DIRS {
            // This loop spreads the task for walking over children among threads.
            // walkdir is only run for immediate children. When  we encounter dirs during a walkdir operation,
            // they are packaged in `dir_sender` queuing them until
            // they are popped for running walkdir on them with `dirs_receiver`.
            let dir_receiver = dirs_receiver.clone();
            let dir_child_sender = dir_child_sender.clone();
            let wg = wg.clone();
            let ign = ign.clone();
            //let running = running.clone();
            s.spawn(move |_| -> eyre::Result<()> {
                // walkdir over the children and send the children for insertion into db using the sender for DirChildren.
                // This is a sort of recursive, but instead of calling itself, it creates and sends a new DirSender for subdirectories within it.
                // Eventually, each such  `DirSender` will be received by some sister thread which calls `walk_recvd_dirs` again on them
                // to send the children of the subdirectory.

                for dir_sender in dir_receiver.iter() {
                    walkdir_from(
                        dir_sender.parent_path(),
                        dir_sender.sender(),
                        dir_child_sender.clone(),
                        &ign,
                    )?;
                }
                drop(dir_receiver);
                drop(dir_child_sender);

                drop(wg);
                Ok(())
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
        .add_line(None, ".tup")?
        .add_line(None, ".git")?
        .add_line(None, ".tupignore")?;
    if root.join(".tupignore").is_file() {
        let err = builder.add(".tupignore");
        if let Some(err) = err {
            return Err(eyre!("unable to add .tupignore: {:?}", err));
        }
    }
    let ign = builder
        .build()
        .map_err(|e| eyre!("unable to build tupignore: {:?}", e))?;
    Ok(ign)
}

// walkdir over the children and send the children for insertion into db using the sender for DirChildren.
// This is a sort of recursive function, but instead of calling itself, it creates and sends a new DirSender for subdirectories within it.
// Eventually, each such  `DirSender` will be received by some thread which calls `walk_recvd_dirs` again on them
// to send the children of the subdirectory.
/*fn walk_recvd_dirs(
    dir_receiver: Receiver<DirSender>,
    dir_child_sender: Sender<DirChildren>,
    ign: &Gitignore,
) -> eyre::Result<()> {
    for dir_sender in dir_receiver.iter() {
        walkdir_from(
            dir_sender.parent_path(),
            dir_sender.sender(),
            dir_child_sender.clone(),
            ign,
        )?;
    }
    drop(dir_receiver);
    drop(dir_child_sender);
    Ok(())
}*/

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
        let mut cnt = 0;
        let now = SystemTime::now();
        for node_at_path in nodereceiver.iter() {
            let node = node_at_path.get_prepared_node();
            //log::debug!("recvd {}, {:?}", node.get_name(), node.get_type());
            let inserted = find_upsert_node(&mut node_statements, &mut add_ids_statements, node)?;
            if node.get_type() == &Dir {
                let id = inserted.get_id();
                dirid_sender.send((node_at_path.get_hashed_path().clone(), id))?;
            }
            cnt += 1;
            progressbar.tick();
        }
        log::debug!(
            "added/modified {} nodes in {} secs",
            cnt,
            now.elapsed().unwrap().as_secs()
        );
    }
    tx.populate_delete_list()?;
    tx.enrich_modified_list_with_outside_mods()?;
    tx.enrich_modified_list()?;
    tx.prune_modified_list()?;
    tx.commit()?;

    Ok(())
}

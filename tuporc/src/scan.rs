use crate::RowType::File;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::{HashMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::Metadata;
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::Deref;
//use std::io::Error as IOError;
use rayon::ThreadPoolBuilder;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::yield_now;
use std::time::{Duration, SystemTime};

use crate::parse::compute_path_hash;
use crate::{is_tupfile, TermProgress};
use crossbeam::channel::{never, tick, Receiver, Sender};
use crossbeam::select;
use crossbeam::sync::WaitGroup;
use eyre::eyre;
use ignore::gitignore::{Gitignore, GitignoreBuilder};
use indicatif::ProgressBar;
use parking_lot::Mutex;
use tupdb::db::{Node, RowType, TupConnection, TupConnectionPool, TupConnectionRef};
use tupdb::deletes::LibSqlDeletes;
use tupdb::inserts::{LibSqlInserts, TupinsertsSql};
use tupdb::queries::LibSqlQueries;
use tupparser::transform::{compute_dir_sha256, compute_sha256};
use walkdir::{DirEntry, WalkDir};

const MAX_THRS_NODES: u8 = 4;
pub const MAX_THRS_DIRS: usize = 24;

pub static TUP_CONFIG: &str = "tup.config";
/// handle the tup scan command by walking the directory tree and adding dirs and files into node table.
pub(crate) fn scan_root(
    root: &Path,
    conn: TupConnectionPool,
    term_progress: &TermProgress,
) -> eyre::Result<()> {
    insert_direntries(root, conn, term_progress)
}

/// mtime stored wrt 1-1-1970
fn time_since_unix_epoch(meta_data: Metadata) -> Option<Duration> {
    meta_data
        .modified()
        .ok()
        .and_then(|st| st.duration_since(SystemTime::UNIX_EPOCH).ok())
}

/// `DirE` is our version of Walkdir's DirEntry. Keeping only the information we are interested in
#[derive(Clone, Debug)]
struct DirE {
    file_name: OsString,
    metadata: Option<Metadata>,
    hashed_path: Option<HashedPath>, // this is an option to enable taking ownership of the path
}

impl DirE {}

impl DirE {
    fn from(de: DirEntry) -> DirE {
        DirE {
            file_name: de.file_name().to_owned(),
            metadata: de.metadata().ok(),
            hashed_path: Some(HashedPath::from(de.into_path())),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn is_symlink(&self) -> bool {
        self.metadata.as_ref().map_or(false, |m| m.is_symlink())
    }

    #[allow(dead_code)]
    pub(crate) fn ends_with(&self, s: &str) -> bool {
        self.file_name.to_string_lossy().ends_with(s)
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
    follow_links: bool,
) {
    let mut children = Vec::new();
    WalkDir::new(root.as_ref())
        .follow_links(follow_links)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .for_each(|e| {
            let dir_entry = DirE::from(e);
            if dir_entry.is_dir() {
                let pp = dir_entry.get_hashed_path();
                if !ign.matched(pp.get_path().as_path(), true).is_ignore() {
                    let pdir = DirSender::new(pp, dir_sender.clone());
                    dir_sender.send(pdir).unwrap(); // unwrap is safe here as we are not expecting the receiver to be closed within scope
                } else {
                    log::debug!("ignoring path:{}", pp.get_path().as_path().display());
                    return;
                }
            }
            children.push(dir_entry);
        });

    dir_entry_sender
        .send(DirChildren::new(root.clone(), children))
        .unwrap();
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
    rtype: &RowType,
) -> Option<NodeAtPath> {
    let mtime = metadata
        .and_then(time_since_unix_epoch)
        .unwrap_or(Duration::from_secs(0))
        .subsec_nanos() as i64;
    Some(create_node_at_path(dirid, file_name, p, mtime, *rtype))
}
/// insert directory entries into Node table if not already added.
fn insert_direntries(
    root: &Path,
    pool: TupConnectionPool,
    term_progress: &TermProgress,
) -> eyre::Result<()> {
    let running = Arc::new(AtomicBool::new(true));
    log::debug!("Inserting/updating directory entries to db");
    {
        let conn = pool.get().expect("unable to get connection from pool");
        tupdb::db::create_temptables(&conn)?;
        let mt = std::fs::metadata(root).ok();
        let root_node = prepare_node_at_path(
            0,
            ".",
            HashedPath::from(root.to_path_buf()),
            mt,
            &RowType::Dir,
        )
        .expect("Unable to prepare root node for insertion");

        let compute_root_sha = || compute_dir_sha256(root).ok();

        let inserted = conn.fetch_upsert_node(&root_node.get_prepared_node(), || {
            compute_root_sha().unwrap_or_default()
        })?;

        let mt = std::fs::metadata(root.join(TUP_CONFIG)).ok();
        if mt.is_some() {
            let tup_config_node = prepare_node_at_path(
                inserted.get_id(),
                TUP_CONFIG,
                HashedPath::from(root.join(TUP_CONFIG)),
                mt,
                &File,
            )
            .expect("Unable to prepare tup.config node for insertion");
            let pathbuf = root.join(TUP_CONFIG);
            let tup_config_sha = || compute_sha256(pathbuf.clone()).unwrap_or_default();
            let _ = conn.fetch_upsert_node(&tup_config_node.get_prepared_node(), tup_config_sha)?;
        }
    }

    let pb = term_progress.pb_main.clone();
    // Begin processing the directory tree
    let thread_pool_builder =
        ThreadPoolBuilder::new().thread_name(|i| format!("tup-scan-thread-{}", i));
    let thread_pool = thread_pool_builder.build()?;
    thread_pool.scope(|s| -> eyre::Result<()> {
        let error = Arc::new(Mutex::from(eyre::Ok(())));
        let wg = WaitGroup::new();
        {
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
                let running = running.clone();
                let ign = ign.clone();
                s.spawn(move |_| {
                    let ticker = tick(Duration::from_millis(500));
                    let mut dirid_receiver = Some(dirid_receiver);
                    let mut dir_children_receiver = Some(dir_children_receiver);
                    let n1 = never();
                    let n2 = never();
                    let mut receiver_cnt = 2;
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
                });
            }
            {
                // threads below convert DirEntries to Nodes ready for sql queries/inserts
                for _ in 0..MAX_THRS_NODES {
                    let ns = nodesender.clone();
                    let dire_receiver = dire_receiver.clone();
                   // let dirid_sender = dirid_sender.clone();
                    //let pool = pool.clone();
                    s.spawn(move |_| {
                        for mut p in dire_receiver.iter() {
                            let dirid = p.get_dir_id();
                            let hashed_path = p.take_path();
                            let metadata = p.take_metadata();
                            let file_name = p.get_file_name().to_string_lossy();
                            let rtype = if metadata.as_ref().map_or(false, |m| m.is_dir()) {
                                RowType::Dir
                            } else {
                                if is_tupfile(file_name.as_ref()) { RowType::TupF } else { File }
                            };
                            let node_at_path =
                                prepare_node_at_path(dirid, file_name, hashed_path, metadata, &rtype);
                            if let Some(node_at_path) = node_at_path {
                                ns.send(node_at_path).expect("Failed to send prepared node");
                            }
                        }
                        drop(ns);
                        log::debug!("finished processing dire entries");
                    });
                }
                drop(nodesender);
            }
            {
                // Sink for the data being transferred over channels processed in different threads.
                // Thread below adds nodes to database for the nodes prepared so far.
                // Nodes are expected to have parent dir ids at this stage.
                // Once a dir is upserted this also sends database ids of dirs so that children of inserted dirs can also be inserted
                let pb = pb.clone();
                let wg = wg.clone();
                let pool = pool.clone();
                let dirid_sender = dirid_sender.clone();
                let term_progress = term_progress.clone();
                let running = running.clone();
                let err = error.clone();
                s.spawn(move |_| {
                    let mut conn = pool.get().expect("unable to get connection from pool");
                    let res = add_modify_nodes(&mut conn, nodereceiver, dirid_sender, &pb);
                    log::debug!("finished adding nodes");
                    if res.is_err() {
                        log::error!("Error:{:?}", res);
                        term_progress.abandon(&pb, " Errors encountered during scanning inserting nodes.");
                        running.store(false, Ordering::SeqCst);
                    } else {
                        term_progress.finish(&pb, "✔ Finished scanning");
                    }
                    let mut e = err.lock();
                    *e= res;
                    drop(wg);
                });
            }
            let (dirs_sender, dirs_receiver) = crossbeam::channel::unbounded();
            let root_hash_path_hash = root_hash_path.get_hash();
            dirs_sender.send(DirSender::new(root_hash_path, dirs_sender.clone()))?;
            drop(dirs_sender);
            for i in 0..MAX_THRS_DIRS {
                // This loop spreads the task of walking over children among MAX_THRS_DIRS threads.
                // walkdir is only run for immediate children. When  we encounter dirs during a walkdir operation,
                // they are packaged in `dir_sender` queuing them until
                // they are popped for running walkdir on them with `dirs_receiver`.
                let dir_receiver = dirs_receiver.clone();
                let dir_child_sender = dir_child_sender.clone();
                let wg = wg.clone();
                let ign = ign.clone();
                //let running = running.clone();
                s.spawn(move |_| {
                    // walkdir over the children and send the children for insertion into db using the sender for DirChildren.
                    // This is a sort of recursive, but instead of calling itself, it creates and sends a new DirSender for subdirectories within it.
                    // Eventually, each such  `DirSender` will be received by some sister thread which calls `walk_recvd_dirs` again on them
                    // to send the children of the subdirectory.

                    for dir_sender in dir_receiver.iter() {
                        let follow_links = dir_sender.parent_path.get_hash().eq(&root_hash_path_hash);
                        walkdir_from(
                            dir_sender.parent_path(),
                            dir_sender.sender(),
                            dir_child_sender.clone(),
                            &ign,
                            follow_links
                        );
                    }
                    drop(dir_receiver);
                    drop(dir_child_sender);
                    drop(wg);
                    log::debug!("Done with thread {}", i);
                });
            }
        }
        wg.wait();
        let e = error.lock();
        if let Err(e) = e.deref() {
            eyre::bail!(e.to_string())
        }
        Ok(())
    })
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
struct NodeUpdateState {
    pub existing_node: Node,
    pub modified: bool,
}

impl NodeUpdateState {
    pub fn new(existing_node: Node, modified: bool) -> NodeUpdateState {
        NodeUpdateState {
            existing_node,
            modified,
        }
    }
    pub(crate) fn get_id(&self) -> i64 {
        self.existing_node.get_id()
    }
    pub(crate) fn is_modified(&self) -> bool {
        self.modified
    }
    pub(crate) fn get_existing_node(&self) -> Node {
        self.existing_node.clone()
    }
}

fn fetch_node_update_state(conn: TupConnectionRef, node: &Node) -> NodeUpdateState {
    conn.fetch_node_by_dir_and_name(node.get_dir(), node.get_name())
        .map_or(
            NodeUpdateState::new(Node::unknown(), true),
            |existing_node| {
                if needs_update(node, &existing_node) {
                    NodeUpdateState {
                        existing_node,
                        modified: true,
                    }
                } else {
                    NodeUpdateState {
                        existing_node,
                        modified: false,
                    }
                }
            },
        )
}
fn needs_update(node: &Node, existing_node: &Node) -> bool {
    if existing_node.get_type().ne(&node.get_type()) {
        return true;
    }
    if (existing_node.get_mtime() - node.get_mtime()).abs() > 1 {
        return true;
    }
    if existing_node.get_display_str() != node.get_display_str() {
        return true;
    }
    if existing_node.get_flags() != node.get_flags() {
        return true;
    }
    if existing_node.get_srcid() != node.get_srcid() {
        return true;
    }
    false
}
#[allow(dead_code)]
fn check_and_send_modified_nodes(
    pool: TupConnectionPool,
    node_at_path: NodeAtPath,
    node_to_insert_sender: Sender<NodeUpdateState>,
    dirid_sender: Sender<(HashedPath, i64)>,
) -> eyre::Result<()> {
    let mut conn = pool.get()?;
    {
        let node = node_at_path.get_prepared_node();
        let mut retries = 5; // Maximum number of retries
        while retries > 0 {
            let tx = conn.deferred_transaction();
            match tx {
                Ok(tx) => {
                    let node_update_sta = fetch_node_update_state(tx.connection(), node);
                    tx.rollback()?;
                    if node.get_type() == &RowType::Dir {
                        let id = node_update_sta.get_id();
                        if id >= 0 {
                            dirid_sender.send((node_at_path.get_hashed_path().clone(), id))?;
                        }
                    }
                    node_to_insert_sender.send(node_update_sta)?;
                    break;
                }
                Err(e) if e.is_busy() => {
                    retries -= 1;
                    log::warn!("Retrying to gain a read lock on database");
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => {
                    return Err(eyre!("unable to gain a read lock on database"));
                }
            }
        }
        drop(node_to_insert_sender);
        drop(dirid_sender);
    }
    Ok(())
}
/// For the received nodes which are already in database (uniqueness of name, dir), this will attempt to update their timestamps.
/// Those not in database will be inserted.
fn add_modify_nodes(
    conn: &mut TupConnection,
    node_at_path_receiver: Receiver<NodeAtPath>,
    dirid_sender: Sender<(HashedPath, i64)>,
    progressbar: &ProgressBar,
) -> eyre::Result<()> {
    let tx = conn.transaction()?;
    {
        let mut cnt = 0;
        let now = SystemTime::now();
        for node_at_path in node_at_path_receiver.iter() {
            let node = node_at_path.get_prepared_node();
            let pbuf = node_at_path.get_hashed_path();
            let node_state = fetch_node_update_state(tx.connection(), node);
            let existing_node = node_state.get_existing_node();
            let is_dir = node.get_type() == &RowType::Dir;
            let id = if node_state.is_modified() {
                let compute_node_sha = || compute_path_hash(is_dir, pbuf.clone());
                let inserted = tx.upsert_node(&node, &existing_node, compute_node_sha)?;
                inserted.get_id()
            } else {
                tx.add_to_present_list(existing_node.get_id(), *existing_node.get_type() as u8)?;
                existing_node.get_id()
            };
            if is_dir {
                dirid_sender.send((pbuf.clone(), id))?;
            }

            cnt += 1;
            progressbar.tick();
        }
        log::debug!(
            "added/modified {} nodes in {} secs",
            cnt,
            now.elapsed()?.as_secs()
        );
    }
    drop(node_at_path_receiver);
    drop(dirid_sender);
    tx.add_not_present_to_delete_list()?;
    // Enrich deletions for nodes whose parent directory entries are missing (orphans)
    tx.enrich_delete_list_for_missing_dirs()?; // this ai says is needed for database integrity
    tx.enrich_delete_list_with_dir_dependents()?;
    tx.delete_nodes()?;
    tx.commit()?;

    Ok(())
}

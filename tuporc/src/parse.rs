//! This module contains functions to parse Tupfiles and add rules to the database
use crate::scan::{HashedPath, MAX_THRS_DIRS};
use crate::TermProgress;
use bimap::BiMap;
use crossbeam::channel::{Receiver, Sender};
use eyre::{bail, eyre, Context, OptionExt, Report, Result};
use indicatif::{ProgressBar};
use log::debug;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use std::fs;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};
use tupdb::db::RowType::{Dir, DirGen, Env, Excluded, File, GenF, Glob, Rule, Task, TupF};
use tupdb::db::{
    create_dirpathbuf_temptable, create_presentlist_temptable, create_tuppathbuf_temptable,
    db_path_str, MiscStatements, Node, RowType, TupConnection, TupConnectionPool, TupConnectionRef,
    TupTransaction,
};
use tupdb::deletes::LibSqlDeletes;
use tupdb::error::{AnyError, DbResult};
use tupdb::inserts::LibSqlInserts;
use tupdb::queries::LibSqlQueries;

use rayon::ThreadPoolBuilder;
use tupparser::buffers::PathBuffers;
use tupparser::buffers::{GlobPath, InputResolvedType, MatchingPath, NormalPath, SelOptions};
use tupparser::decode::{OutputHandler, PathDiscovery, PathSearcher};
use tupparser::errors::Error;
use tupparser::transform::{
    compute_dir_hash, compute_glob_hash, compute_hash, StatementsToResolve,
};
use tupparser::{
    EnvDescriptor, GlobPathDescriptor, GroupPathDescriptor, PathDescriptor, RuleDescriptor,
    RuleRefDescriptor, TaskDescriptor, TupPathDescriptor,
};
use tupparser::{ReadWriteBufferObjects, ResolvedRules, TupParser};
use RowType::Group;

pub fn cancel_flag() -> &'static Arc<AtomicBool> {
    static CANCEL_FLAG: OnceLock<Arc<AtomicBool>> = OnceLock::new();
    CANCEL_FLAG.get_or_init(|| {
        let flag = Arc::new(AtomicBool::new(false));
        let handler_flag = flag.clone();
        if let Err(e) = ctrlc::try_set_handler(move || {
            eprintln!("Ctrl-C pressed, cancelling...");
            handler_flag.store(true, Ordering::SeqCst)
        }) {
            log::warn!("Ctrl-C handler already set: {}", e);
        }
        flag
    })
}

// CrossRefMaps maps paths, groups and rules discovered during parsing with those found in database
// These are two ways maps, so you can query both ways
#[derive(Debug, Clone, Default)]
pub struct CrossRefMaps {
    group_id: BiMap<GroupPathDescriptor, (i64, i64)>,
    // group id and the corresponding db id, parent id
    path_id: BiMap<PathDescriptor, (i64, i64)>,
    // path id and the corresponding db id, parent id (includes globs)
    rule_id: BiMap<RuleDescriptor, (i64, i64)>,
    // rule id and the corresponding db id, parent id
    tup_id: BiMap<TupPathDescriptor, (i64, i64)>,
    // tup id and the corresponding db id, parent id
    env_id: BiMap<EnvDescriptor, i64>, // env id and the corresponding db id
    // task id and the corresponding db id, parent id
    task_id: BiMap<TaskDescriptor, (i64, i64)>,
}

#[derive(Debug, Clone)]
pub enum SrcId {
    RuleId(RuleDescriptor),
}
#[derive(Debug, Clone)]
pub(crate) enum NodeToInsert {
    Rule(RuleDescriptor),
    #[allow(dead_code)]
    Tup(TupPathDescriptor),
    Group(GroupPathDescriptor),
    GeneratedFile(PathDescriptor, SrcId, Option<String>),
    Env(EnvDescriptor),
    Task(TaskDescriptor),
    InputFile(PathDescriptor, Option<String>),
    Glob(GlobPathDescriptor, TupPathDescriptor, Option<String>),
    ExcludedFile(PathDescriptor),
    #[allow(dead_code)]
    Dir(PathDescriptor, Option<String>),
    #[allow(dead_code)]
    DirGen(PathDescriptor, Option<String>),
}

impl PathDiscovery for ConnWrapper<'_, '_> {
    fn discover_paths_with_cb(
        &self,
        path_buffers: &impl PathBuffers,
        glob_path: &[GlobPath],
        cb: impl FnMut(MatchingPath) -> Result<(), Error> + Send + Sync,
        sel: SelOptions,
    ) -> Result<usize, Error> {
        DbPathSearcher::get_glob_matches(self.conn, path_buffers, glob_path, cb, sel)
    }
}

impl NodeToInsert {
    /// get the name of the node to insert in the database
    pub fn get_name(&self, read_write_buffer_objects: &ReadWriteBufferObjects) -> String {
        match self {
            NodeToInsert::Rule(r) => read_write_buffer_objects.get_rule(r).get_rule_str(),
            NodeToInsert::Tup(t) => read_write_buffer_objects
                .get_tup_path(t)
                .file_name()
                .to_string(),
            NodeToInsert::Group(g) => read_write_buffer_objects.get_group_name(g),
            NodeToInsert::GeneratedFile(p, _, _) => read_write_buffer_objects
                .get_path(p)
                .file_name()
                .to_string(),
            NodeToInsert::Env(e) => read_write_buffer_objects.get_env_name(e),
            NodeToInsert::Task(t) => read_write_buffer_objects.get_task(t).to_string(),
            NodeToInsert::InputFile(p, _) => read_write_buffer_objects
                .get_path(p)
                .file_name()
                .to_string(),
            NodeToInsert::Glob(g, _, _) => read_write_buffer_objects.get_name(g),
            NodeToInsert::ExcludedFile(e) => read_write_buffer_objects.get_name(e),
            NodeToInsert::Dir(p, _) => read_write_buffer_objects.get_name(p),
            NodeToInsert::DirGen(p, _) => read_write_buffer_objects.get_name(p),
        }
    }

    #[allow(dead_code)]
    fn get_input_file(&self) -> Option<PathDescriptor> {
        match self {
            NodeToInsert::InputFile(p, _) => Some(p.clone()),
            _ => None,
        }
    }

    pub fn get_id(&self) -> i64 {
        match self {
            NodeToInsert::Rule(r) => r.to_u64() as i64,
            NodeToInsert::Tup(t) => t.to_u64() as i64,
            NodeToInsert::Group(g) => g.to_u64() as i64,
            NodeToInsert::GeneratedFile(p, _, _) => p.to_u64() as i64,
            NodeToInsert::Env(e) => e.to_u64() as i64,
            NodeToInsert::Task(t) => t.to_u64() as i64,
            NodeToInsert::InputFile(p, _) => p.to_u64() as i64,
            NodeToInsert::Glob(g, _, _) => g.to_u64() as i64,
            NodeToInsert::ExcludedFile(p) => p.to_u64() as i64,
            NodeToInsert::Dir(p, _) => p.to_u64() as i64,
            NodeToInsert::DirGen(p, _) => p.to_u64() as i64,
        }
    }

    pub fn get_row_type(&self) -> RowType {
        match self {
            NodeToInsert::Rule(_) => Rule,
            NodeToInsert::Tup(_) => TupF,
            NodeToInsert::Group(_) => Group,
            NodeToInsert::GeneratedFile(_, _, _) => GenF,
            NodeToInsert::Env(_) => Env,
            NodeToInsert::Task(_) => Task,
            NodeToInsert::InputFile(_, _) => File,
            NodeToInsert::Glob(_, _, _) => Glob,
            NodeToInsert::ExcludedFile(_) => Excluded,
            NodeToInsert::Dir(_, _) => Dir,
            NodeToInsert::DirGen(_, _) => DirGen,
        }
    }

    pub fn get_srcid(
        &self,
        cross_ref_maps: &CrossRefMaps,
        read_write_buffer_objects: &ReadWriteBufferObjects,
    ) -> Result<i64> {
        match self {
            NodeToInsert::Rule(r) => {
                let tup_id = read_write_buffer_objects.get_rule(r).get_tup_file_desc();
                cross_ref_maps
                    .get_tup_db_id(&tup_id)
                    .map(|x| x.0)
                    .ok_or_eyre(eyre!("srcid not found for rule: {:?}", r,))
            }
            NodeToInsert::Task(t) => {
                let tupid = read_write_buffer_objects.get_task(t).get_tupfile_desc();
                cross_ref_maps
                    .get_tup_db_id(&tupid)
                    .map(|x| x.0)
                    .ok_or_eyre(eyre!(
                        "srcid not found for task: {:?} with tup descriptor:{:?}",
                        t,
                        tupid
                    ))
            }
            NodeToInsert::GeneratedFile(gen, SrcId::RuleId(id), _) => cross_ref_maps
                .get_rule_db_id(id)
                .map(|x| x.0)
                .ok_or_eyre(eyre!(
                    "srcid not found for generated file: {:?} with rule descriptor:{:?}",
                    gen,
                    id
                )),

            NodeToInsert::Glob(g, tupid, _) => cross_ref_maps
                .get_tup_db_id(tupid)
                .map(|x| x.0)
                .ok_or_eyre(eyre!(
                    "srcid not found for glob: {:?} with tup descriptor:{:?}",
                    g,
                    tupid
                )),

            _ => Ok(-1),
        }
    }
    pub fn get_display_str(&self, read_write_buffer_objects: &ReadWriteBufferObjects) -> String {
        match self {
            NodeToInsert::Rule(r) => read_write_buffer_objects.get_rule(r).get_display_str(),
            NodeToInsert::Env(e) => read_write_buffer_objects.get_env_value(e),
            NodeToInsert::Glob(g, _, _) => read_write_buffer_objects.get_recursive_glob_str(g),
            _ => "".to_string(),
        }
    }
    fn get_flags(&self, read_write_buffer_objects: &ReadWriteBufferObjects) -> String {
        match self {
            NodeToInsert::Rule(r) => read_write_buffer_objects
                .get_rule(r)
                .get_flags()
                .to_string(),
            _ => Default::default(),
        }
    }
    pub fn get_parent_id(
        &self,
        read_write_buffer_objects: &ReadWriteBufferObjects,
    ) -> PathDescriptor {
        match self {
            NodeToInsert::Rule(r) => read_write_buffer_objects.get_rule(r).get_parent_id(),
            NodeToInsert::Tup(t) => read_write_buffer_objects.get_tup_parent_id(t),
            NodeToInsert::Group(g) => read_write_buffer_objects.get_group_parent_id(g),
            NodeToInsert::GeneratedFile(p, _, _) => read_write_buffer_objects.get_parent_id(p),
            NodeToInsert::Env(_) => PathDescriptor::default(),
            NodeToInsert::Task(t) => read_write_buffer_objects.get_task(t).get_parent_id(),
            NodeToInsert::InputFile(p, _) => read_write_buffer_objects.get_parent_id(p),
            NodeToInsert::Glob(g, _, _) => read_write_buffer_objects.get_glob_prefix(g),
            NodeToInsert::ExcludedFile(e) => read_write_buffer_objects.get_parent_id(e),
            NodeToInsert::Dir(p, _) => read_write_buffer_objects.get_parent_id(p),
            NodeToInsert::DirGen(p, _) => read_write_buffer_objects.get_parent_id(p),
        }
    }

    pub fn compute_node_sha(
        &self,
        conn: &TupConnectionRef,
        path_buffers: &impl PathBuffers,
    ) -> Option<String> {
        if let Some(value) = self.get_saved_hash() {
            return Some(value);
        }
        let compute_sha_for = |p: &PathDescriptor| {
            let p = PathBuffers::get_abs_path(path_buffers, p);
            let sha = compute_hash(p.as_path()).ok();
            debug!("sha for {:?} is {:?}", p, sha);
            sha
        };
        let compute_sha_for_glob = |p: &PathDescriptor| {
            let sha = compute_glob_hash(&ConnWrapper::new(conn), path_buffers, p).ok();
            debug!("sha for {:?} is {:?}", p, sha);
            sha
        };

        let compute_sha_for_dir = |p: &GlobPathDescriptor| {
            let p = path_buffers.get_abs_path(p);
            let sha = compute_dir_hash(p.as_path()).ok();
            debug!("sha for {:?} is {:?}", p, sha);
            sha
        };
        match self {
            NodeToInsert::GeneratedFile(p, _, _) => compute_sha_for(p),
            NodeToInsert::Tup(p) => compute_sha_for(p),
            NodeToInsert::InputFile(p, _) => compute_sha_for(p),
            NodeToInsert::Glob(g, _, _) => compute_sha_for_glob(g),
            NodeToInsert::DirGen(p, _) => compute_sha_for_dir(p),
            NodeToInsert::Dir(p, _) => compute_sha_for_dir(p),
            _ => None,
        }
    }

    fn force_save_hash(&mut self, conn: &TupConnectionRef, path_buffers: &impl PathBuffers) {
        if self.get_saved_hash().is_none() {
            let mut sha = self.compute_node_sha(conn, path_buffers);
            match self {
                NodeToInsert::InputFile(_, h) => *h = sha.take(),
                NodeToInsert::Glob(_, _, h) => *h = sha.take(),
                NodeToInsert::Dir(_, h) => *h = sha.take(),
                NodeToInsert::DirGen(_, h) => *h = sha.take(),
                NodeToInsert::GeneratedFile(_, _, h) => *h = sha.take(),
                _ => {}
            }
        }
    }

    fn get_saved_hash(&self) -> Option<String> {
        match self {
            NodeToInsert::InputFile(_, Some(hash)) => return Some(hash.clone()),
            NodeToInsert::Glob(_, _, Some(hash)) => return Some(hash.clone()),
            NodeToInsert::Dir(_, Some(hash)) => return Some(hash.clone()),
            NodeToInsert::DirGen(_, Some(hash)) => return Some(hash.clone()),
            NodeToInsert::GeneratedFile(_, _, Some(has)) => return Some(has.clone()),
            _ => {}
        }
        None
    }

    // use this only in the order of nodes returned by the parser
    // for example when inserting nodes in the database, rules should be inserted first before outputs.
    // that way the srcid of outputs can be resolved via crossref
    pub fn get_node(
        &self,
        read_write_buffer_objects: &ReadWriteBufferObjects,
        crossref: &CrossRefMaps,
    ) -> Result<Node> {
        debug!("building node for {:?}", self);
        let parent_id = self.get_parent_id(read_write_buffer_objects);
        let (parent_id, _) = crossref.get_path_db_id(&parent_id).ok_or_eyre(eyre!(
            "parent id not found when building node {:?}",
            parent_id
        ))?;
        Ok(match self.get_row_type() {
            DirGen | Dir | Excluded => Node::new(
                self.get_id(),
                parent_id,
                0,
                self.get_name(read_write_buffer_objects),
                self.get_row_type(),
            ),
            Rule => Node::new_rule(
                self.get_id(),
                parent_id,
                self.get_name(read_write_buffer_objects),
                self.get_display_str(read_write_buffer_objects),
                self.get_flags(read_write_buffer_objects),
                self.get_srcid(crossref, read_write_buffer_objects)? as _,
            ),
            TupF | File | GenF | Glob => {
                Node::new_file_or_genf(
                    self.get_id(),
                    parent_id,
                    0,
                    self.get_name(read_write_buffer_objects),
                    self.get_row_type(),
                    self.get_srcid(crossref, read_write_buffer_objects)?, // at this point we know that rules have been inserted it is generated
                )
            }
            Env => Node::new_env(
                self.get_id(),
                parent_id,
                self.get_name(read_write_buffer_objects),
                self.get_display_str(read_write_buffer_objects),
            ),
            Group => Node::new_grp(
                self.get_id(),
                parent_id,
                self.get_name(read_write_buffer_objects),
            ),
            Task => Node::new_task(
                self.get_id(),
                parent_id,
                self.get_name(read_write_buffer_objects),
                "".to_string(),
                "".to_string(),
                self.get_srcid(crossref, read_write_buffer_objects)? as _,
            ),
        })
    }

    pub fn update_crossref(&self, crossref: &mut CrossRefMaps, id: i64, parid: i64) {
        match self {
            NodeToInsert::Rule(r) => {
                crossref.add_rule_xref(r.clone(), id, parid);
            }
            NodeToInsert::Tup(td) => {
                crossref.add_tup_xref(td.clone(), id, parid);
            }
            NodeToInsert::Group(g) => {
                crossref.add_group_xref(g.clone(), id, parid);
            }
            NodeToInsert::GeneratedFile(p, _, _) => {
                crossref.add_path_xref(p.clone(), id, parid);
            }
            NodeToInsert::Env(e) => {
                crossref.add_env_xref(e.clone(), id);
            }
            NodeToInsert::Task(t) => {
                crossref.add_task_xref(t.clone(), id, parid);
            }
            NodeToInsert::InputFile(p, _) => {
                crossref.add_path_xref(p.clone(), id, parid);
            }
            NodeToInsert::Glob(g, _, _) => {
                crossref.add_path_xref(g.clone(), id, parid);
            }
            NodeToInsert::ExcludedFile(p) => {
                crossref.add_path_xref(p.clone(), id, parid);
            }
            NodeToInsert::Dir(d, _) => {
                crossref.add_path_xref(d.clone(), id, parid);
            }
            NodeToInsert::DirGen(d, _) => {
                crossref.add_path_xref(d.clone(), id, parid);
            }
        }
    }
    pub fn get_depth(&self, read_write_buffer_objects: &ReadWriteBufferObjects) -> usize {
        self.get_parent_id(read_write_buffer_objects).depth() + 1
    }
    
    pub fn get_type(&self) -> RowType {
        self.get_row_type()
    }
}
impl CrossRefMaps {
    fn get_group_db_id(&self, g: &GroupPathDescriptor) -> Option<(i64, i64)> {
        self.group_id.get_by_left(g).copied()
    }
    pub fn get_group_db_id_ok(&self, g: &GroupPathDescriptor) -> Result<(i64, i64)> {
        self.get_group_db_id(g)
            .ok_or_else(|| eyre!("group {} not found in crossref", g))
    }

    fn get_path_db_id(&self, p: &PathDescriptor) -> Option<(i64, i64)> {
        if p.eq(&PathDescriptor::default()) {
            Some((1, 0))
        } else {
            self.path_id.get_by_left(p).copied()
        }
    }
    pub fn get_path_db_id_ok(&self, p: &PathDescriptor) -> Result<(i64, i64)> {
        self.get_path_db_id(p)
            .ok_or_else(|| eyre!("path {} not found in crossref", p))
    }
    fn get_rule_db_id(&self, r: &RuleDescriptor) -> Option<(i64, i64)> {
        self.rule_id.get_by_left(r).copied()
    }

    pub fn get_rule_db_id_ok(&self, r: &RuleDescriptor) -> Result<(i64, i64)> {
        self.get_rule_db_id(r)
            .ok_or_else(|| eyre!("rule {} not found in crossref", r))
    }

    fn get_tup_db_id(&self, r: &TupPathDescriptor) -> Option<(i64, i64)> {
        self.tup_id.get_by_left(r).copied()
    }

    pub fn get_tup_db_id_ok(&self, r: &TupPathDescriptor) -> Result<(i64, i64)> {
        self.get_tup_db_id(r)
            .ok_or_else(|| eyre!("tup {} not found in crossref", r))
    }
    fn get_env_db_id(&self, e: &EnvDescriptor) -> Option<i64> {
        self.env_id.get_by_left(e).copied()
    }

    pub fn get_env_db_id_ok(&self, e: &EnvDescriptor) -> Result<i64> {
        self.get_env_db_id(e)
            .ok_or_else(|| eyre!("env {} not found in crossref", e))
    }
    fn get_glob_db_id(&self, s: &GlobPathDescriptor) -> Option<(i64, i64)> {
        self.path_id.get_by_left(&s).copied()
    }
    pub fn get_glob_db_id_ok(&self, s: &GlobPathDescriptor) -> Result<(i64, i64)> {
        self.get_glob_db_id(s)
            .ok_or_else(|| eyre!("glob {} not found in crossref", s))
    }
    #[allow(dead_code)]
    pub fn get_task_id(&self, t: &TaskDescriptor) -> Option<(i64, i64)> {
        self.task_id.get_by_left(t).copied()
    }

    pub fn add_group_xref(&mut self, g: GroupPathDescriptor, db_id: i64, par_db_id: i64) {
        self.group_id.insert(g, (db_id, par_db_id));
    }
    pub fn add_env_xref(&mut self, e: EnvDescriptor, db_id: i64) {
        self.env_id.insert(e, db_id);
    }

    pub fn add_path_xref(&mut self, p: PathDescriptor, db_id: i64, par_db_id: i64) {
        self.path_id.insert(p, (db_id, par_db_id));
    }
    pub fn add_rule_xref(&mut self, r: RuleDescriptor, db_id: i64, par_db_id: i64) {
        self.rule_id.insert(r, (db_id, par_db_id));
    }
    pub fn add_tup_xref(&mut self, t: TupPathDescriptor, db_id: i64, par_db_id: i64) {
        self.tup_id.insert(t, (db_id, par_db_id));
    }
    pub fn add_task_xref(&mut self, t: TaskDescriptor, db_id: i64, par_db_id: i64) {
        self.task_id.insert(t.into(), (db_id, par_db_id));
    }
}
/// wait for the next [StatementsToResolve] and process them
fn receive_resolved_statements(
    parser: &TupParser<DbPathSearcher>,
    receiver: Receiver<StatementsToResolve>,
    pb_ref: &ProgressBar,
    term_progress: &TermProgress,
    conn_ref: &TupConnectionRef,
    rwbufs: &ReadWriteBufferObjects,
    poisoned: &Arc<AtomicBool>,
    insert_sender: &Sender<TupBuildGraphSnapshot>,
) -> Result<()> {
    while let Ok(to_resolve) = receiver.recv() {
        let tup_desc = to_resolve.get_cur_file_desc().clone();
        log::info!("resolving statements for tupfile {:?}", tup_desc);
        let (resolved_rules, _) = parser
            .process_raw_statements(to_resolve)
            .wrap_err(format!(
                "While processing statements for tupfile {}",
                tup_desc
            ))
            .inspect_err(|e| log::error!("error found resolving stmts in {tup_desc} -- {e}"))?;
        pb_ref.set_message(format!("Resolving {}", resolved_rules.get_tupid()));
        let _cancelled = check_cancel()?;
        if poisoned.load(Ordering::SeqCst) {
            break;
        }
        term_progress.update_pb(&pb_ref);
        collect_db_insertions(
            &conn_ref,
            resolved_rules,
            &rwbufs,
            insert_sender,
        )?;
    }
    Ok(())
}

fn into_any_error(e: Error) -> AnyError {
    AnyError::from(e.to_string())
}
/// Path searcher that scans the sqlite database for matching paths
/// It is used by the parser to resolve paths and globs. Resolved outputs are dumped in OutputHolder
#[derive(Clone)]
struct DbPathSearcher {
    conn: TupConnectionPool,
    root: PathBuf,
}

impl DbPathSearcher {
    pub fn new<P: AsRef<Path>>(conn: TupConnectionPool, root: P) -> DbPathSearcher {
        DbPathSearcher {
            conn,
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn fetch_glob_globs_cb<F>(
        conn: &TupConnectionRef,
        ph: &impl PathBuffers,
        glob_paths: &[GlobPath],
        ref mut f: F,
        sel: SelOptions,
    ) -> Result<usize, AnyError>
    where
        F: FnMut(MatchingPath) -> Result<(), Error> + Send + Sync,
    {
        let mut num_matches = 0;
        for glob_path in glob_paths {
            let has_glob_pattern = glob_path.has_glob_pattern();
            if has_glob_pattern {
                debug!(
                    "looking for matches in db for glob pattern: {:?}",
                    glob_path.get_abs_path()
                );
            }

            let non_pattern_path = glob_path.get_non_pattern_prefix_desc();
            debug!("base path:{:?}", non_pattern_path);
            let tup_cwd = glob_path.get_tup_dir_desc();
            //debug!("base path is : {:?}", ph.get_path(base_path));
            let glob_pattern = ph.get_rel_path(&glob_path.get_glob_path_desc(), non_pattern_path);
            debug!("glob pattern is : {:?}", glob_pattern);
            if glob_pattern.to_string().contains("$(") {
                log::error!("unexpected path to search!");
            }
            let fetch_row = |n: Node| -> DbResult<()> {
                let s = n.get_name();
                if n.get_type().is_dir() && !sel.allows_dir() {
                    return Ok(());
                }
                if n.get_type().is_file() && !sel.allows_file() {
                    return Ok(());
                }
                debug!("found glob match:{} in db", s);
                let full_path_pd = ph.add_abs(s).map_err(into_any_error)?;
                let matching_path = if has_glob_pattern {
                    let full_path_pd_clone = full_path_pd.clone();
                    let full_path = ph.get_path(&full_path_pd);
                    if glob_path.is_match(full_path.as_path()) {
                        let grps = glob_path.group(full_path.as_path());
                        debug!("found match: {:?} for {:?}", grps, glob_path);
                        Some(MatchingPath::with_captures(
                            full_path_pd_clone,
                            glob_path.get_glob_desc(),
                            grps,
                            tup_cwd.clone(),
                        ))
                    } else {
                        None
                    }
                } else if !has_glob_pattern {
                    Some(MatchingPath::new(full_path_pd, tup_cwd.clone()))
                } else {
                    None
                };
                if let Some(mp) = matching_path {
                    num_matches = num_matches + 1;
                    f(mp).map_err(|e| AnyError::from(e.to_string()))?;
                }
                Ok(())
            };

            conn.fetch_glob_matches(
                non_pattern_path.get_path_ref().as_path(),
                &glob_pattern.to_string(),
                fetch_row,
                2,
            )?;
            if num_matches != 0 {
                return Ok(num_matches);
            }
        }
        log::warn!("no rows found for any glob pattern: {:?}", glob_paths);
        Err(AnyError::query_returned_no_rows())
    }

    fn get_glob_matches(
        tup_connection_ref: &TupConnectionRef,
        path_buffers: &impl PathBuffers,
        glob_path: &[GlobPath],
        cb: impl FnMut(MatchingPath) -> Result<(), Error> + Send + Sync,
        sel: SelOptions,
    ) -> Result<usize, Error> {
        if glob_path.is_empty() {
            return Ok(0);
        }
        let first_glob = &glob_path[0];
        let match_count =
            Self::fetch_glob_globs_cb(&tup_connection_ref, path_buffers, glob_path, cb, sel)
                .map_err(|e| match e {
                    _ if e.is_query_returned_no_rows() => {
                        Error::NoGlobMatches(first_glob.get_glob_desc().get_path_ref().to_string())
                    }
                    _ => Error::new_callback_error(e.to_string()),
                })?;
        Ok(match_count)
    }
}
impl PathDiscovery for DbPathSearcher {
    fn discover_paths_with_cb(
        &self,
        path_buffers: &impl PathBuffers,
        glob_path: &[GlobPath],
        cb: impl FnMut(MatchingPath) -> Result<(), Error> + Send + Sync,
        sel: SelOptions,
    ) -> Result<usize, Error> {
        let conn = self
            .conn
            .get()
            .map_err(|e| Error::new_callback_error(e.to_string()))?;
        let tup_connection_ref = conn.as_ref();
        Self::get_glob_matches(&tup_connection_ref, path_buffers, &glob_path, cb, sel)
    }

    fn discover_paths(
        &self,
        path_buffers: &impl PathBuffers,
        output_handler: &impl OutputHandler,
        glob_path: &[GlobPath],
        sel: SelOptions,
    ) -> Result<Vec<MatchingPath>, Error> {
        let mut mps = Vec::new();
        Self::discover_paths_with_cb(
            self,
            path_buffers,
            glob_path,
            |mp| {
                mps.push(mp);
                Ok(())
            },
            sel.clone(),
        )
        .or_else(|e| {
            if e.is_no_glob_matches() {
                Ok(0)
            } else {
                Err(e)
            }
        })?;
        let c = |x: &MatchingPath, y: &MatchingPath| {
            let x = x.path_descriptor();
            let y = y.path_descriptor();
            x.cmp(&y)
        };
        if mps.is_empty() {
            if sel.allows_file() {
                mps = output_handler.discover_paths(path_buffers, glob_path)?;
            }
        }
        mps.sort_by(c);
        mps.dedup();
        Ok(mps)
    }
}
impl PathSearcher for DbPathSearcher {
    fn locate_tup_rules(
        &self,
        tup_cwd: &PathDescriptor,
        path_buffers: &impl PathBuffers,
    ) -> Vec<PathDescriptor> {
        let conn = self.conn.get().expect("connection not found");
        let tup_path = tup_cwd.get_path_ref();
        debug!(
            "tup path is : {} in which (or its parents) we look for TupRules.tup or TupRules.lua",
            tup_path.as_path().display()
        );
        let dirid = conn
            .deref()
            .fetch_dirid_by_path(tup_path.as_path())
            .unwrap_or(1i64);
        debug!("dirid: {}", dirid);
        let mut tup_rules = Vec::new();
        let tuprs = ["TupRules.tup", "TupRules.lua"];
        let mut add_rules = |dirid| {
            for tupr in tuprs {
                if let Ok((dirid_, name)) = conn
                    .fetch_closest_parent(tupr, dirid)
                    .inspect_err(|e| eprintln!("Error while looking for TupRules: {}", e))
                {
                    debug!("tup rules node  : {} dir:{}", name, dirid_);
                    let node_dir = conn.fetch_dirpath(dirid_)
                        .unwrap_or_else(|e|
                            panic!("Directory {dirid_} not found while trying to locate TupRules. Error {e}")
                        );
                    let node_path = node_dir.join(name.as_str());
                    let _ = path_buffers
                        .add_abs(node_path.as_path())
                        .map(|pd| tup_rules.push(pd));
                    break;
                }
            }
        };
        add_rules(dirid);
        tup_rules
    }

    fn get_root(&self) -> &Path {
        self.root.as_path()
    }

    fn is_dir(&self, pd: &PathDescriptor) -> (bool, i64) {
        let conn = self.conn.get().expect("connection not found");
        conn.fetch_dirid_by_path(pd.get_path_ref())
            .map(|id| (true, id))
            .unwrap_or((false, -1))
    }
}

pub(crate) fn parse_tupfiles_in_db_for_dump<P: AsRef<Path>>(
    connection: TupConnectionPool,
    tupfiles: Vec<Node>,
    root: P,
    term_progress: &TermProgress,
    var: &String,
) -> Result<Vec<(TupPathDescriptor, String)>> {
    {
        let db = DbPathSearcher::new(connection, root.as_ref());
        let mut parser = TupParser::try_new_from(root.as_ref(), db)?;
        let cnt = tupfiles
            .iter()
            .filter(|x| !x.get_name().ends_with(".lua"))
            .count();
        let pb = term_progress.make_child_len_progress_bar("_", cnt as u64);
        let mut states_after_parse = Vec::new();
        {
            for tupfile in tupfiles
                .iter()
                .filter(|x| !x.get_name().ends_with(".lua"))
                .cloned()
            {
                pb.set_message(format!("Parsing :{}", tupfile.get_name()));
                let res = parser.parse_tupfile_immediate(tupfile.get_name());
                let statements_to_resolve = match res {
                    Ok(s) => s,
                    Err(e) => {
                        log::error!(
                            "Error parsing tupfile {}: {}",
                            tupfile.get_name(),
                            e.to_string()
                        );

                        term_progress
                            .abandon(&pb, format!("Error parsing tupfile {}", tupfile.get_name()));
                        return Err(Report::from(e));
                    }
                };
                if let Some(val) = statements_to_resolve.fetch_var(var) {
                    states_after_parse
                        .push((statements_to_resolve.get_cur_file_desc().clone(), val));
                }
                pb.set_message(format!("Done parsing {}", tupfile.get_name()));
                term_progress.progress(&pb);
            }
        }
        Ok(states_after_parse)
    }
}

/// handle the tup parse command which assumes files in db and adds rules and makes links joining input and output to/from rule statements
pub(crate) fn parse_tupfiles_in_db<P: AsRef<Path>>(
    connection: TupConnectionPool,
    tupfiles: Vec<Node>,
    root: P,
    keep_going: bool,
) -> Result<()> {
    if tupfiles.is_empty() {
        log::warn!("No Tupfiles to parse");
        return Ok(());
    }
    let term_progress =  TermProgress::new("Parsing tupfiles");

    {
        let mut conn = connection.get()?;
        let tx = conn.transaction()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        tx.set_run_status("parse", "in_progress", now)?;
        tx.commit()?;
        tupdb::db::create_temptables(&conn)?; // for PresentList, TupfileEntries
    }
    //conn.prune_modified_list_basic()?;
    let db = DbPathSearcher::new(connection.clone(), root.as_ref());
    let parser = TupParser::try_new_from(root.as_ref(), db)?;
    //let term_progress = term_progress.set_main_with_ticker("Parsing tupfiles");
    {
        let dbref = parser.get_searcher();
        let conn = dbref.conn.get()?;
        //conn.enrich_modify_list()?;
        {
            conn.mark_rules_with_changed_io()
                .context("Mark rules with changed I/O")?;
            term_progress.update_main();

            conn.mark_group_deps().context("Mark group dependencies")?;
            term_progress.update_main();
            conn.prune_modify_list().context("Prune modify list")?;
            term_progress.update_main();
        }
        let tupfile_ids = tupfiles.iter().map(|t| t.get_id());
        conn.mark_tupfile_outputs(tupfile_ids)
            .context("Mark tupfile outputs")?;
    }
    let res = parse_and_add_rules_to_db(parser, tupfiles.as_slice(), term_progress.clone(), keep_going);
    match res {
        Err(e) => {
            log::error!("Error during parsing tupfiles: {}", e);
            term_progress.abandon_main("Error ");
            return Err(e);
        }
        _ => {
            term_progress.finish_main("Parsing completed");
        }
    }
    {
        let conn = connection.get()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        conn.set_run_status("parse", "success", now)?;
    }
    Ok(())
}

fn insert_link(tx: &TupTransaction, link: &Link, crossref: &mut CrossRefMaps) -> Result<()> {
    match link {
        Link::InputToRule(i, rd) => {
            let input_id = crossref.get_path_db_id_ok(&i)?;
            let rule_id = crossref.get_rule_db_id_ok(&rd)?;
            tx.insert_link(input_id.0, rule_id.0, false.into(), Rule)?;
        }
        Link::RuleToOutput(rd, out) => {
            let rule_id = crossref.get_rule_db_id_ok(&rd)?;
            let output_id = crossref.get_path_db_id_ok(&out)?;
            // Enforce unique producer per output (Rule or Task): check if an existing producer exists
            {
                let mut stmt = tx
                    .prepare("SELECT from_id FROM LiveNormalLink WHERE to_id = ?1 AND to_type IN (4, 7) LIMIT 1")
                    .map_err(|e| eyre!("DB prepare failed while checking producer uniqueness: {}", e))?;
                let mut rows = stmt.query([output_id.0]).map_err(|e| {
                    eyre!("DB query failed while checking producer uniqueness: {}", e)
                })?;
                if let Some(row) = rows
                    .next()
                    .map_err(|e| eyre!("DB row fetch failed: {}", e))?
                {
                    let existing_from_id: i64 = row
                        .get(0)
                        .map_err(|e| eyre!("DB column get failed: {}", e))?;
                    if existing_from_id != rule_id.0 {
                        return Err(eyre!(
                            "Output (id={}) already has a producer (id={}), cannot also be produced by Rule (id={})",
                            output_id.0,
                            existing_from_id,
                            rule_id.0
                        ));
                    }
                }
            }
            // Determine actual output node type (GenF or DirGen) for link's to_type
            let out_node = tx
                .connection()
                .fetch_node_by_id(output_id.0)
                .map_err(|e| eyre!("DB fetch failed while getting output node type: {}", e))?
                .ok_or_else(|| {
                    eyre!(
                        "Output node id {} not found while linking rule",
                        output_id.0
                    )
                })?;
            let out_ty = *out_node.get_type();
            tx.insert_link(rule_id.0, output_id.0, true.into(), out_ty)?;
        }
        Link::OutputToGroup(out, group) => {
            let output_id = crossref.get_path_db_id_ok(&out)?;
            let group_id = crossref.get_group_db_id_ok(&group)?;
            tx.insert_link(output_id.0, group_id.0, true.into(), Group)?;
        }
        Link::GroupToRule(group, rd) => {
            let group_id = crossref.get_group_db_id_ok(&group)?;
            let rule_id = crossref.get_rule_db_id_ok(&rd)?;
            tx.insert_link(group_id.0, rule_id.0, false.into(), Rule)?;
        }
        Link::EnvToRule(env, rd) => {
            let env_id = crossref.get_env_db_id_ok(&env)?;
            let rule_id = crossref.get_rule_db_id_ok(&rd)?;
            tx.insert_link(env_id, rule_id.0, true.into(), Rule)?;
        }
        Link::TupfileToTupfile(pd, tup_desc) => {
            let tupfile_id = crossref.get_tup_db_id_ok(&tup_desc)?;
            let tupfile_read_id = crossref.get_path_db_id_ok(&pd)?;
            tx.insert_link(tupfile_read_id.0, tupfile_id.0, true.into(), TupF)?;
        }
        Link::GlobToTupfile(g, tup_desc) => {
            let glob_id = crossref.get_glob_db_id_ok(&g)?;
            let tupfile_id = crossref.get_tup_db_id_ok(&tup_desc)?;
            tx.insert_link(glob_id.0, tupfile_id.0, true.into(), TupF)?;
        }
        Link::TupfileToRule(tup_desc, rd) => {
            let tupfile_id = crossref.get_tup_db_id_ok(&tup_desc)?;
            let rule_id = crossref.get_rule_db_id_ok(&rd)?;
            tx.insert_link(tupfile_id.0, rule_id.0, false.into(), Rule)?;
        }
        Link::DirToGlob(dir, glob, depth) => {
            let dir_id = crossref.get_path_db_id_ok(&dir)?;
            let glob_id = crossref.get_glob_db_id_ok(&glob)?;
            let more_dirs = fetch_deeper_dirs(&tx.connection(), glob_id.0, *depth)?;
            tx.insert_link(dir_id.0, glob_id.0, true.into(), Glob)?;
            for d in more_dirs {
                tx.insert_link(d, glob_id.0, true.into(), Glob)?;
            }
        }
    }
    Ok(())
}

fn insert_links(
    tx: &mut TupTransaction,
    //resolved_rules: &ResolvedRules,
    tup_id: &TupPathDescriptor,
    crossref: &mut CrossRefMaps,
    link_collector: &LinkCollector,
) -> Result<()> {
    // collect all un-added groups and add them in a single transaction.
    for l in link_collector.links() {
        insert_link(&tx, l, crossref)
            .context(format!("Inserting link {} in tupfile {}", l, tup_id,))?;
    }
    Ok(())
}

fn add_links_from_to_rules_and_groups(
    resolved_rules: &ResolvedRules,
    link_collector: &mut LinkCollector,
) {
    for rl in resolved_rules.get_resolved_links().iter() {
        let rd = rl.get_rule_desc();
        let rule_ref = rl.get_rule_ref();
        let tup_desc = rule_ref.get_tupfile_desc();
        link_collector.add_tupfile_to_rule(&tup_desc, rd);
        for p in rl.get_targets() {
            link_collector.add_output_to_rule_link(rd, p);
            if let Some(group_desc) = rl.get_group_desc() {
                link_collector.add_output_to_group_link(p, &group_desc);
            }
        }
        for s in rl.get_sources() {
            link_collector.add_input_to_rule_link(s, rd);
            link_collector.add_group_to_rule_link(s, rd);
        }
        for env in rl.get_env_list().iter() {
            link_collector.add_env_to_rule_link(&env, rd);
        }
    }
}

fn collect_links_from_to_globs(
    resolved_rules: &ResolvedRules,
    link_collector: &mut LinkCollector,
) -> Result<(), Report> {
    for glob_desc in resolved_rules.get_globs_read() {
        let tup_desc = resolved_rules.get_tupid();
        link_collector.add_glob_to_tupfile_link(glob_desc, tup_desc);
        let glob_path = GlobPath::build_from(&PathDescriptor::default(), glob_desc)?;
        let non_pattern_path = glob_path.get_non_pattern_prefix_desc();
        let depth = glob_desc.components().count() - non_pattern_path.components().count();
        if depth > 1 {
            link_collector.add_dir_to_glob_link_deep(non_pattern_path, glob_desc, depth);
        } else {
            link_collector.add_dir_to_glob_link(non_pattern_path, glob_desc);
        }
    }
    Ok(())
}

fn fetch_deeper_dirs(
    conn: &TupConnectionRef,
    glob_dir: i64,
    depth: usize,
) -> Result<Vec<i64>, Report> {
    let mut links = Vec::new();
    conn.for_each_glob_dir(glob_dir, depth as i32, |id: i64| -> DbResult<()> {
        links.push(id);
        //link_collector.add_dir_to_glob_link(&dir_path, resolved_rules.get_tupid());
        Ok(())
    })
    .wrap_err_with(|| eyre!("Adding glob paths at {}", glob_dir,))?;
    Ok(links)
}

// Gather all the modified tupfiles within directories specified
// Processing Tupfiles within small set of directories with leave db in an incomplete state but is useful for debugging
pub fn gather_modified_tupfiles(
    conn: &mut TupConnection,
    targets: &Vec<String>,
    inspect_dir_path_buf: bool,
) -> Result<Vec<Node>> {
    let mut tupfiles = Vec::new();

    let term_progress = TermProgress::new("Gathering modified Tupfiles");
    term_progress.pb_main.set_message("Creating temp tables");
    let time = create_dirpathbuf_temptable(conn)
        .with_context(|| "Failed to create and fill dirpathbuf table")?;
    #[cfg(debug_assertions)]
     term_progress.set_message("Created dirpathbuf table in {time:?}");
    let _ = time;
    term_progress.update_main();

    create_tuppathbuf_temptable(conn)
        .with_context(|| "Failed to create and fill tuppathbuf table")?;
    term_progress.update_main();

    create_presentlist_temptable(conn)?;

    term_progress.update_main();
    // trigger reparsing of Tupfiles which contain included modified Tupfiles or modified globs
    conn.mark_tupfile_deps()
        .context("Mark tupfile dependencies")?; //-- included tup files -> Tupfile
    for (i, sha) in conn.fetch_modified_globs()?.into_iter() {
        conn.update_node_sha(i, sha.as_str())?;
        conn.mark_glob_deps(i).context("Mark glob dependencies")?; // modified glob -> Tupfile
    }
    term_progress.update_main();

    let mut target_dirs_and_names = Vec::new();
    if !targets.is_empty() {
        //let find_dir_id = conn.fetch_dirid_prepare()?;
        for target in targets.iter() {
            if target.trim().is_empty() {
                continue;
            }
            let dir_path = NormalPath::new_from_cow_str(Cow::from(target.as_str()));
            target_dirs_and_names.push(db_path_str(dir_path.as_path()));
        }
        for t in target_dirs_and_names.iter() {
            log::info!("target dir for tupfile parse:{}", t);
        }
    }
    if inspect_dir_path_buf {
        return Ok(tupfiles);
    }
    term_progress
        .pb_main
        .set_message("Looking up modified Tupfiles");
    conn.for_each_modified_tupfile(|n: Node| {
        // name stores full path here
        debug!("tupfile to parse:{}", n.get_name());
        if target_dirs_and_names.is_empty()
            || target_dirs_and_names
                .iter()
                .any(|x| n.get_name().strip_prefix(x.as_str()).is_some())
        {
            tupfiles.push(n);
        }
        Ok(())
    })?;
    term_progress.finish_main("Gathered modified Tupfiles");
    Ok(tupfiles)
}

pub fn gather_tupfiles(mut conn: TupConnection) -> Result<Vec<Node>> {
    let mut tupfiles = Vec::new();
    create_dirpathbuf_temptable(&mut conn)?;

    conn.for_each_tupnode(|n: Node| {
        // the field `name` in n stores full path here
        tupfiles.push(n);
        Ok(())
    })?;
    Ok(tupfiles)
}

fn parse_and_add_rules_to_db(
    mut parser: TupParser<DbPathSearcher>,
    tupfiles: &[Node],
    term_progress: TermProgress,
    keep_going: bool,
) -> Result<TupParser<DbPathSearcher>> {
    let cancel_flag = cancel_flag();
    cancel_flag.store(false, Ordering::SeqCst);
    //let term_progress = term_progress.set_main_with_ticker("Parsing Tupfiles");
    //let mut del_stmt = conn.delete_tup_rule_links_prepare()?;
    let (sender, receiver) = crossbeam::channel::unbounded();
    let mut crossref = CrossRefMaps::default();
    for tup_node in tupfiles.iter() {
        let tupid = parser
            .read_write_buffers()
            .add_tup_file(Path::new(tup_node.get_name()));
        crossref.add_tup_xref(tupid, tup_node.get_id(), tup_node.get_dir());
    }
    let pool_threads = {
        let pool_size = parser.get_searcher().conn.max_size() as usize;
        // leave a couple of connections for insert/other work, and cap to MAX_THRS_DIRS
        std::cmp::max(1, std::cmp::min(MAX_THRS_DIRS, pool_size.saturating_sub(2)))
    };
    let thread_pool = ThreadPoolBuilder::new()
        .thread_name(|i| format!("tup-parse-thread-{i}"))
        .num_threads(pool_threads)
        .build()?;
    let parse_result = Arc::new(Mutex::new(None));
    thread_pool.scope(|scope| {
        if let Err(e) = parse_in_scope(
            scope,
            &mut parser,
            tupfiles,
            &term_progress,
            sender,
            receiver,
            &mut crossref,
            keep_going,
        ) {
            *parse_result.lock().expect("poisoned parse result mutex") = Some(e);
            term_progress.abandon_main("Error during parsing");
        }
        else {
            term_progress.finish_main("Done");
        }
    });
    if let Some(err) = parse_result
        .lock()
        .expect("poisoned parse result mutex")
        .take()
    {
        return Err(err);
    }
    Ok(parser)
}
fn check_cancel() -> Result<()> {
    let cancel = cancel_flag();
    if cancel.load(Ordering::SeqCst) {
        return Err(eyre!("Insertion interrupted by user (Ctrl-C)"));
    }
    Ok(())
}
/// A snapshot of the nodes and links to insert for a single tupfile
#[derive(Debug, Clone)]
struct TupBuildGraphSnapshot {
    tup_id: TupPathDescriptor,
    envs_to_insert: HashSet<EnvDescriptor>,
    nodes_to_insert: Vec<NodeToInsert>,
    link_collector: LinkCollector,
}
impl TupBuildGraphSnapshot {
    fn new(
        tup_id: TupPathDescriptor,
        envs_to_insert: HashSet<EnvDescriptor>,
        nodes_to_insert: Vec<NodeToInsert>,
        link_collector: LinkCollector,
    ) -> Self {
        TupBuildGraphSnapshot {
            tup_id,
            envs_to_insert,
            nodes_to_insert,
            link_collector,
        }
    }
    fn get_tup_id(&self) -> &TupPathDescriptor {
        &self.tup_id
    }
    fn get_envs_to_insert(&self) -> &HashSet<EnvDescriptor> {
        &self.envs_to_insert
    }
    fn get_nodes_to_insert(&self) -> &Vec<NodeToInsert> {
        &self.nodes_to_insert
    }
    fn get_links(&self) -> &LinkCollector {
        &self.link_collector
    }
}
// subroutine to build graph snapshot to insert for a given set of resolved rules
fn collect_db_insertions(
    conn_ref: &TupConnectionRef,
    resolved_rules: ResolvedRules,
    rwbufs: &ReadWriteBufferObjects,
    insert_sender: &Sender<TupBuildGraphSnapshot>,
) -> Result<()> {
    let (envs_to_insert, mut nodes) =
        collect_nodes_to_insert(&rwbufs, &resolved_rules, check_cancel)?;

    for node in nodes.iter_mut() {
        node.force_save_hash(conn_ref, rwbufs.get());
    }
    let mut link_collector = LinkCollector::new();
    for tupfile_read in resolved_rules.get_tupfiles_read() {
        link_collector.add_tupfile_to_tupfile_link(tupfile_read, resolved_rules.get_tupid());
    }
    collect_links_from_to_globs(&resolved_rules, &mut link_collector)
        .context("Collecting links from/to globs")?;
    add_links_from_to_rules_and_groups(&resolved_rules, &mut link_collector);

    insert_sender.send(TupBuildGraphSnapshot::new(
        resolved_rules.get_tupid().clone(),
        envs_to_insert,
        nodes,
        link_collector,
    ))?;
    Ok(())
}

// Spawn multiple threads to parse tupfiles in parallel within a scope
fn record_error(store: &Arc<Mutex<Option<Report>>>, err: Report) {
    let mut guard = store.lock().expect("poisoned error store");
    if guard.is_none() {
        *guard = Some(err);
    }
}

fn parse_in_scope<'scope, 'b>(
    s: &rayon::Scope<'scope>,
    parser: &'scope mut TupParser<DbPathSearcher>,
    tupfiles: &'b [Node],
    term_progress: &TermProgress,
    sender: Sender<StatementsToResolve>,
    receiver: Receiver<StatementsToResolve>,
    mut crossref: &'scope mut CrossRefMaps,
    keep_going: bool,
) -> Result<(), Report>
where
    'b: 'scope,
{
    let term_progress = term_progress.clone();

    term_progress.suspend();
    let poisoned = Arc::new(AtomicBool::new(false));
    let first_err = Arc::new(Mutex::new(None));
    let total_tupfiles = tupfiles
        .iter()
        .filter(|x| !x.get_name().ends_with(".lua"))
        .count();
   let num_lua = tupfiles
            .iter()
            .filter(|x| x.get_name().ends_with(".lua"))
            .count();
    let parse_pb = term_progress.make_child_len_progress_bar(
        "Parsing tupfiles-1",
        std::cmp::max(1, total_tupfiles) as u64,
    );
    let resolve_pb = term_progress.make_child_len_progress_bar(
        "Resolving statements",
        std::cmp::max(1, total_tupfiles) as u64,
    );
    let lua_pb = if num_lua == 0 {
        None
    } else {
        Some(term_progress.make_child_len_progress_bar(
            "Parsing lua build files",
            num_lua as u64,
        ))
    };
    let lua_pb_main = lua_pb.clone();
    let pb_insert = term_progress.make_child_len_progress_bar("Inserting nodes", 1);
    term_progress.set_message("Starting..");

    tupfiles
        .iter()
        .filter(|x| !x.get_name().ends_with(".lua"))
        .cloned()
        .for_each(|tupfile| {
            let parser_clone = parser.clone();
            let sender = sender.clone();
            let pb = parse_pb.clone();
            let poisoned = poisoned.clone();
            let first_err = first_err.clone();
            let term_progress = term_progress.clone();
            s.spawn(move |_| {
                if let Err(e) = parse_one_tupfile(
                    tupfile,
                    &term_progress,
                    parser_clone,
                    sender,
                    &pb,
                    poisoned,
                    keep_going,
                ) {
                    record_error(&first_err, e);
                }
            });
        });
    drop(sender);
    // create threads to receive resolved statements and batch insert them to db
    let (insert_sender, insert_receiver) = crossbeam::channel::unbounded::<TupBuildGraphSnapshot>();
    {
        let poisoned = poisoned.clone();
        let first_err = first_err.clone();
        let non_lua_count = tupfiles
            .iter()
            .filter(|x| !x.get_name().ends_with(".lua"))
            .count();
        let num_receivers = std::cmp::max(
            1,
            std::cmp::min(rayon::current_num_threads(), std::cmp::max(1, non_lua_count)),
        );

        for _worker_idx in 0..num_receivers {
            let parser_c = parser.clone();
            let pb_ref = resolve_pb.clone();
            let insert_sender = insert_sender.clone();
            let rwbufs = parser_c.read_write_buffers().clone();
            let receiver = receiver.clone();
            let poisoned = poisoned.clone();
            let first_err = first_err.clone();
            let term_progress = term_progress.clone();
            s.spawn(move |_| {
                let result = (|| -> Result<()> {
                    let conn = parser_c.get_searcher().conn.get()?;
                    let conn_ref = conn.as_ref();
                    let status = receive_resolved_statements(
                        &parser_c,
                        receiver,
                        &pb_ref,
                        &term_progress,
                        &conn_ref,
                        &rwbufs,
                        &poisoned,
                        &insert_sender,
                    )
                    .context("Resolving statements in tupfiles");
                    drop(insert_sender);
                    handle_result(&term_progress, &poisoned, &pb_ref, status)
                })();
                if let Err(e) = result {
                    record_error(&first_err, e);
                }
            });
        }

        if num_lua != 0 {
            let lua_pb_worker = lua_pb.clone();
            let mut parser_c = parser.clone();
            let insert_sender = insert_sender.clone();
            let rwbufs = parser_c.read_write_buffers().clone();
            let term_progress = term_progress.clone();
            let poisoned = poisoned.clone();
            let first_err = first_err.clone();
            s.spawn(move |_| {
                let res = (|| -> Result<()> {
                    let pb_ref = lua_pb_worker.clone().unwrap();
                    let conn = parser_c.get_searcher().conn.get()?;
                    for tupfile_lua in tupfiles
                        .iter()
                        .filter(|x| x.get_name().ends_with(".lua"))
                        .cloned()
                    {
                        check_cancel()?;
                        if poisoned.load(Ordering::SeqCst) {
                            debug!("parsing lua build files cancelled");
                            break;
                        }
                        let path = tupfile_lua.get_name();
                        pb_ref.set_message(format!("Parsing :{}", path));
                        let (resolved_rules, _) = parser_c.parse(path).map_err(|e| {
                            eyre!("Error: {}", parser_c.read_write_buffers().display_str(&e))
                        })?;
                        let conn_ref = conn.as_ref();
                        collect_db_insertions(
                            &conn_ref,
                            resolved_rules,
                            &rwbufs,
                            &insert_sender,
                        )?;
                        pb_ref.inc(1);
                        pb_ref.set_message(format!("Done parsing {}", path));
                    }
                    drop(insert_sender);
                    pb_ref.finish_and_clear();
                    Ok(())
                })();
                if let Err(e) = handle_result(&term_progress, &poisoned, lua_pb.as_ref().unwrap(), res) {
                    record_error(&first_err, e);
                }
            });
        }
        drop(insert_sender);
    }
    {
        let parser_c = parser.clone();
        let rwbufs = parser.read_write_buffers().clone();

        let psx = parser_c.get_searcher();
        let mut c = psx.conn.get()?;
        let mut tx = c.transaction()?;
        let ticker = crossbeam::channel::tick(std::time::Duration::from_millis(500));
        let mut loop_aborted = false;
        loop {
            crossbeam::select! {
                recv(ticker) -> _ => {
                    if poisoned.load(Ordering::SeqCst) {
                        debug!("recvd user interrupt");
                        loop_aborted = true;
                        break;
                    }
                    if check_cancel().is_err() {
                        debug!("insertion cancelled");
                        loop_aborted = true;
                        break;
                    }
                    continue;
                },

                recv(insert_receiver) -> nodes_batch_res => {
                    let nodes_batch = match nodes_batch_res {
                        Ok(nb) => nb,
                        Err(_) => {
                            log::info!("No more nodes to insert, finishing up");
                            break;
                        }
                    };
                    let tup_link_info = nodes_batch;
                    let tup_id = tup_link_info.get_tup_id().clone();
                    insert_nodes_wrapper(
                        &mut tx,
                        &term_progress,
                        &mut crossref,
                        &rwbufs,
                        &pb_insert,
                        &tup_id,
                        tup_link_info.get_envs_to_insert(),
                        &tup_link_info.get_nodes_to_insert(),
                    )?;
                    let (dbid, _) = crossref
                        .get_tup_db_id(&tup_id)
                        .ok_or_else(|| eyre!("Could not fetch db id for tupfile {}", &tup_id))?;
                    tx.unmark_modified(dbid)?;
                    //check_uniqueness_of_parent_rule(&tx.connection(), &rwbufs, &outs, crossref)?;
                    let _ = insert_links(&mut tx, &tup_id, crossref, tup_link_info.get_links())
                        .context(format!("Inserting links for {}", tup_id))?;
                    term_progress.update_pb(&pb_insert);
                } // end recv of insert_receiver
            } // end select
        }
        if loop_aborted {
            let _ = tx.rollback();
            term_progress.abandon(&pb_insert, "Insertion aborted");
            if let Some(err) = first_err.lock().expect("poisoned error store").take() {
                return Err(err);
            }
            return Ok(());
        }
        tx.mark_absent_tupfile_entries_to_delete().context("Mark absent rules to delete list")?;
        tx.mark_orphans_to_delete().context("Mark outputs left without producers")?; // cascade deletion of orphaned nodes
        log::info!("Marked orphaned entries to delete");
        tx.delete_nodes()?; // delete all marked nodes
        tx.prune_delete_list()?; // clean up delete list after deletions
        log::info!("Deleted marked nodes");
        tx.commit()?;
        pb_insert.finish_and_clear();
    }

    parse_pb.finish_and_clear();
    resolve_pb.finish_and_clear();
    if let Some(pb) = lua_pb_main {
        pb.finish_and_clear();
    }
    let pb = term_progress.get_main();
    term_progress.finish(&pb, "Done parsing tupfiles");
    term_progress.clear();
    log::info!("Finished parse phase");
    log::info!("Finished resolving statements from parsed tupfiles");
    if let Some(err) = first_err.lock().expect("poisoned error store").take() {
        return Err(err);
    }
    Ok(())
}

fn handle_result(
    term_progress: &TermProgress,
    poisoned: &Arc<AtomicBool>,
    pb_ref: &ProgressBar,
    status: Result<()>,
) -> Result<()> {

    match status {
        Err(e) => {
            log::error!("Error: {}", e);
            term_progress.abandon(&pb_ref, format!("Aborted processing parsed tupfiles : {e}"));
            poisoned.store(true, Ordering::SeqCst);
            Err(e)
        }
        _ => {
            // expected to be cleared already
            Ok(())
        }
    }
}

fn insert_nodes_wrapper(
    tx: &mut TupTransaction,
    term_progress: &TermProgress,
    mut crossref: &mut CrossRefMaps,
    rwbufs: &ReadWriteBufferObjects,
    pb_insert: &ProgressBar,
    tup_id: &TupPathDescriptor,
    envs_to_insert: &HashSet<EnvDescriptor>,
    nodes: &Vec<NodeToInsert>,
) -> Result<(), Report> {
    //let mut tx = c.transaction()?;
    let insert_label = format!(
        "Inserting nodes for {}",
        rwbufs.get_tup_path(tup_id).as_path().display()
    );
    log::info!("{}", insert_label);
    // If a Tupfile produced no rules, mark it resolved (unmark modified) but warn.
    if nodes.is_empty() {
        log::info!(
            "Tupfile {} produced no rules; marking as resolved",
            insert_label
        );
        return Ok(());
    }
    insert_nodes(
        tx,
        &rwbufs,
        &mut crossref,
        term_progress,
        &pb_insert,
        insert_label.as_str(),
        &nodes,
        &envs_to_insert,
    )?;
    Ok(())
}

// parse a single tupfile
fn parse_one_tupfile(
    tupfile: Node,
    term_progress: &TermProgress,
    mut parser_clone: TupParser<DbPathSearcher>,
    sender: Sender<StatementsToResolve>,
    pb: &ProgressBar,
    poisoned: Arc<AtomicBool>,
    keep_going: bool, // keep going on error
) -> Result<()> {
    let tupfile_name = tupfile.get_name();
    if poisoned.load(Ordering::SeqCst) {
        drop(sender);
        term_progress.abandon(&pb, format!("Aborted parsing {tupfile_name}"));
        bail!("Parsing aborted due to error or cancellation");
    }
    log::info!("Parsing :{}", tupfile_name);
    pb.set_message(format!("Parsing :{tupfile_name}"));
    match parser_clone
        .parse_tupfile(tupfile.get_name(), sender.clone())
        .map_err(|error| {
            let rwbuf = parser_clone.read_write_buffers();
            let display_str = rwbuf.display_str(&error);
            eyre!(
                "Failed to parse tupfile {}: {}\nCaused by: {}",
                tupfile.get_name(),
                display_str,
                error
            )
        }) {
        Ok(_) => {
            pb.set_message(format!("Done parsing {}", tupfile.get_name()));
            log::info!("Finished parsing :{}", tupfile_name);
            term_progress.update_pb(&pb);
        }
        Err(e) if keep_going => {
            log::error!("{}", e);
            pb.set_message(format!("Error parsing {}", tupfile.get_name()));
            term_progress.update_pb(&pb);
        }
        Err(e) => {
            log::error!("{}", e);
            drop(sender);
            term_progress.abandon(&pb, format!("Error parsing {tupfile_name}"));
            poisoned.store(true, Ordering::SeqCst);
            return Err(e);
        }
    }
    drop(sender);
    log::info!("Finished parsing tupfile {}", tupfile_name);
    Ok(())
}

/// find node by path
fn find_by_path(path: &Path, conn: &TupConnectionRef) -> Result<Node> {
    let res = find_node_id_by_path(conn, path).and_then(|node| {
        conn.fetch_node_by_id(node)
            .transpose()
            .expect("Unexpected failure in retrieving node from its id")
    })?;
    Ok(res)
}

// find node id by path
fn find_node_id_by_path(conn: &TupConnectionRef, path: &Path) -> Result<i64, AnyError> {
    path.components()
        .try_fold((0, PathBuf::new()), |acc, c| {
            let (dir, path_so_far) = acc;
            let name = c.as_os_str().to_string_lossy();
            let node = conn.fetch_node_id_by_dir_and_name(dir, name.as_ref())?;
            Ok((node, path_so_far.join(name.as_ref())))
        })
        .map(|(id, _)| id)
}

/// removes the path from the database
/// uses the transaction's connection and does not commit.
pub(crate) fn remove_path_tx<P: AsRef<Path>>(tx: &TupTransaction, path: P) -> Result<()> {
    let connection_ref = tx.connection();
    if let Ok(node) = find_by_path(path.as_ref(), &connection_ref) {
        tx.mark_deleted(node.get_id(), node.get_type())?;
    }
    Ok(())
}

/// inserts a path into the database,
/// After insertion, the path also appears in crossref maps as a bimap source and target <-> db_id
pub(crate) fn insert_path(
    tx: &TupTransaction,
    path_buffers: &impl PathBuffers,
    pd: &PathDescriptor,
    cross_ref_maps: &mut CrossRefMaps,
    rtype: RowType,
    srcid: i64,
) -> Result<(i64, i64)> {
    if let Some((id, parent_id)) = cross_ref_maps.get_path_db_id(pd) {
        return Ok((id, parent_id));
    }
    let parent_pd = path_buffers.get_parent_id(pd);
    let rtype_parent = if rtype.is_generated() {
        DirGen // if the node parent is not inserted yet, it is a generated directory
    } else {
        Dir
    };

    // Walk the parent components, inserting any missing directory with the correct parent id/path.
    let (mut cur_parent_id, mut cur_path) = (1i64, path_buffers.get_root_dir().to_path_buf());
    for comp in parent_pd.components() {
        let name = comp.get_file_name_os_str();
        if comp.is_root() || name == "." {
            continue; // skip synthetic root or explicit "."
        }
        cur_path.push(name);
        let next_id = if let Some((path_db_id, _)) = cross_ref_maps.get_path_db_id(&comp) {
            path_db_id
        } else {
            let (inserted_id, _) = insert_node_in_dir(
                tx,
                name.to_string_lossy(),
                cur_path.as_path(),
                cur_parent_id,
                &rtype_parent,
                -1,
            )?;
            cross_ref_maps.add_path_xref(comp.clone(), inserted_id, cur_parent_id);
            inserted_id
        };
        cur_parent_id = next_id;
    }

    // Finally insert the path itself (if still missing) and record it.
    if let Some((id, parent_id)) = cross_ref_maps.get_path_db_id(pd) {
        return Ok((id, parent_id));
    }

    let name = pd.get_file_name_os_str();
    let (nid, _) = insert_node_in_dir(
        tx,
        name.to_string_lossy(),
        path_buffers.get_path_ref(pd),
        cur_parent_id,
        &rtype,
        srcid,
    )?;
    cross_ref_maps.add_path_xref(pd.clone(), nid, cur_parent_id);
    Ok((nid, cur_parent_id))
}

/// inserts a node into the database. This is an upsert operation and will not modify the node if it already exists with same values
fn insert_node_in_dir(
    tx: &TupTransaction,
    name: Cow<str>,
    path: &Path,
    dir: i64,
    rtype: &RowType,
    srcid: i64,
) -> Result<(i64, i64)> {
    let pbuf = path.to_owned();
    let hashed_path = HashedPath::from(pbuf);
    let mut metadata = fs::metadata(path).ok();
    if metadata.as_ref().map_or(false, |m| m.is_symlink()) {
        metadata = fs::symlink_metadata(path).ok();
    }
    let missing_metadata = metadata.is_none();
    let is_dir = metadata.as_ref().map_or(false, |m| m.is_dir());
    let node_at_path = crate::scan::prepare_node_at_path(
        dir,
        name.clone(),
        hashed_path.clone(),
        metadata,
        &rtype,
        srcid,
    );
    if rtype.eq( &File) || rtype.eq(&TupF) && missing_metadata {
        return Err(eyre!(
            "Cannot insert file or tupfile node for missing path: {}",
            path.display()
        ));
    }
    let in_node = node_at_path.get_prepared_node();
    let pbuf = node_at_path.get_hashed_path().clone();
    let node = tx.fetch_upsert_node_raw(in_node, || compute_path_hash(is_dir, pbuf.clone()))?;
    Ok((node.get_id(), dir))
}

pub(crate) fn compute_path_hash(is_dir: bool, pbuf: HashedPath) -> String {
    if is_dir {
        compute_dir_hash(pbuf.as_ref()).unwrap_or_default()
    } else {
        compute_hash(pbuf.as_ref()).unwrap_or_default()
    }
}

/// Before we insert a child node, insert its parent and record its db id in cross_ref_maps against parent's PathDescriptor
fn ensure_parent_inserted(
    tx: &TupTransaction,
    path_buffers: &impl PathBuffers,
    cross_ref_maps: &mut CrossRefMaps,
    rtype: &RowType,
    parent: &PathDescriptor,
) -> Result<(i64, i64), Report> {
    let pardir_type = if is_generated(&rtype) { DirGen } else { Dir };
    let parent_path = path_buffers.get_path(parent);
    if pardir_type.eq(&DirGen) {
        // make sure directory is physically present for generated files
        let parent_path_from_root = path_buffers.get_root_dir().join(parent_path);
        if !parent_path_from_root.exists() {
            fs::create_dir_all(parent_path_from_root).map_err(|e| eyre!(e.to_string()))?;
        }
    }

    if let Some((dir, pardir)) = cross_ref_maps.get_path_db_id(parent) {
        return Ok((dir, pardir));
    }

    let (dir, pardir) = find_by_path(parent_path.as_path(), &tx.connection())
        .map(|n| (n.get_id(), n.get_dir()))
        .or_else(|_| -> Result<(i64, i64)> {
            // try adding parent directory if not in db
            let (dir, pardir) =
                insert_path(tx, path_buffers, parent, cross_ref_maps, pardir_type, -1)?;
            Ok((dir, pardir))
        })?;
    Ok((dir, pardir))
}

//  Collector records the work done in inserting/verifying correctness of tup nodes into db.
#[derive(Debug, Clone)]
struct Collector {
    processed_globs: HashSet<PathDescriptor>,
    processed: HashSet<PathDescriptor>,
    processed_groups: HashSet<GroupPathDescriptor>,
    processed_outputs: HashSet<PathDescriptor>,
    nodes_to_insert: Vec<NodeToInsert>,
    unique_rule_check: HashMap<String, RuleRefDescriptor>,
    read_write_buffer_objects: ReadWriteBufferObjects,
    processed_envs: HashSet<EnvDescriptor>,
}
// Various types of links that are inserted into db

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum Link {
    DirToGlob(PathDescriptor, GlobPathDescriptor, usize),
    InputToRule(PathDescriptor, RuleDescriptor),
    RuleToOutput(RuleDescriptor, PathDescriptor),
    OutputToGroup(PathDescriptor, GroupPathDescriptor),
    GroupToRule(GroupPathDescriptor, RuleDescriptor),
    EnvToRule(EnvDescriptor, RuleDescriptor),
    TupfileToTupfile(PathDescriptor, TupPathDescriptor),
    GlobToTupfile(GlobPathDescriptor, TupPathDescriptor),
    TupfileToRule(TupPathDescriptor, RuleDescriptor),
}

impl Display for Link {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Link::DirToGlob(d, g, _) => { write!(f, "DirToGlob({}->{})", d, g) },
            Link::InputToRule(i, r) => { write!(f, "InputToRule({}->{})", i, r) },
            Link::RuleToOutput(r, o) => { write!(f, "RuleToOutput({}->{})", r, o) },
            Link::OutputToGroup(o, g) => { write!(f, "OutputToGroup({}->{})", o, g) },
            Link::GroupToRule(g, r) => { write!(f, "GroupToRule({}->{})", g, r) },
            Link::EnvToRule(e, r) => { write!(f, "EnvToRule({}->{})", e, r) },
            Link::TupfileToTupfile(t0, t2) => { write!(f, "TupfileToTupfile({}->{})", t0, t2) },
            Link::GlobToTupfile(g, t) => { write!(f, "GlobToTupfile({}->{})", g, t) },
            Link::TupfileToRule(g, r) => {
                write!(f, "TupfileToRule({}->{})", g, r)
            }
        }
    }
}
/// Placeholder for all types of links that would go into db in the NormalLink table.
#[derive(Debug, Clone, Default)]
struct LinkCollector {
    links: HashSet<Link>,
}
impl LinkCollector {
    pub(crate) fn new() -> LinkCollector {
        LinkCollector::default()
    }
    pub(crate) fn add_output_to_rule_link(&mut self, rd: &RuleDescriptor, output: &PathDescriptor) {
        self.links
            .insert(Link::RuleToOutput(rd.clone(), output.clone()));
    }

    pub(crate) fn add_dir_to_glob_link(&mut self, d: &PathDescriptor, g: &GlobPathDescriptor) {
        self.links.insert(Link::DirToGlob(d.clone(), g.clone(), 1));
    }
    pub(crate) fn add_dir_to_glob_link_deep(
        &mut self,
        d: &PathDescriptor,
        g: &GlobPathDescriptor,
        depth: usize,
    ) {
        self.links
            .insert(Link::DirToGlob(d.clone(), g.clone(), depth));
    }

    pub(crate) fn add_input_to_rule_link(&mut self, inp: &InputResolvedType, rd: &RuleDescriptor) {
        inp.get_resolved_path_desc().map(|p| {
            self.links.insert(Link::InputToRule(p.clone(), rd.clone()));
        });
    }
    pub(crate) fn add_output_to_group_link(
        &mut self,
        output: &PathDescriptor,
        g: &GroupPathDescriptor,
    ) {
        self.links
            .insert(Link::OutputToGroup(output.clone(), g.clone()));
    }
    pub(crate) fn add_env_to_rule_link(&mut self, e: &EnvDescriptor, rd: &RuleDescriptor) {
        self.links.insert(Link::EnvToRule(e.clone(), rd.clone()));
    }
    pub(crate) fn add_tupfile_to_tupfile_link(
        &mut self,
        t: &PathDescriptor,
        tup: &TupPathDescriptor,
    ) {
        self.links
            .insert(Link::TupfileToTupfile(t.clone(), tup.clone()));
    }
    pub(crate) fn add_glob_to_tupfile_link(
        &mut self,
        g: &GlobPathDescriptor,
        tup: &TupPathDescriptor,
    ) {
        self.links
            .insert(Link::GlobToTupfile(g.clone(), tup.clone()));
    }

    pub(crate) fn add_group_to_rule_link(&mut self, i: &InputResolvedType, rd: &RuleDescriptor) {
        i.get_group_ref().map(|g| {
            self.links.insert(Link::GroupToRule(g.clone(), rd.clone()));
        });
    }
    pub(crate) fn add_tupfile_to_rule(&mut self, t: &TupPathDescriptor, rd: &RuleDescriptor) {
        self.links
            .insert(Link::TupfileToRule(t.clone(), rd.clone()));
    }

    pub(crate) fn links(&self) -> impl Iterator<Item = &Link> {
        self.links.iter()
    }
}

impl Collector {
    pub(crate) fn new(read_write_buffer_objects: ReadWriteBufferObjects) -> Result<Collector> {
        Ok(Collector {
            processed_globs: HashSet::new(),
            processed: HashSet::new(),
            processed_groups: Default::default(),
            processed_outputs: Default::default(),
            nodes_to_insert: Vec::new(),
            processed_envs: HashSet::new(),
            //existing_nodeids: BTreeSet::new(),
            unique_rule_check: HashMap::new(),
            read_write_buffer_objects,
        })
    }
    fn nodes(self) -> Vec<NodeToInsert> {
        self.nodes_to_insert
    }
    fn collect_output(&mut self, p: &PathDescriptor, srcid: SrcId) -> Result<()> {
        let root = self.read_write_buffer_objects.get_root();
        let hash = compute_hash::<&Path>(root.join(p.get_path_ref()).as_ref()).ok();
        self.nodes_to_insert
            .push(NodeToInsert::GeneratedFile(p.clone(), srcid, hash));
        Ok(())
    }

    pub(crate) fn add_output(&mut self, p0: &PathDescriptor, id: SrcId) -> Result<()> {
        if self.processed_outputs.insert(p0.clone()) {
            self.collect_output(p0, id)
        } else {
            Err(eyre!(
                "output already processed: {:?}",
                p0.get_file_name().to_string()
            ))
        }
    }

    fn collect_glob(&mut self, p: &GlobPathDescriptor, tupid: TupPathDescriptor) -> Result<()> {
        self.nodes_to_insert
            .push(NodeToInsert::Glob(p.clone(), tupid, None));
        Ok(())
    }

    fn collect_excluded(&mut self, p: &PathDescriptor) -> Result<()> {
        self.nodes_to_insert
            .push(NodeToInsert::ExcludedFile(p.clone()));
        Ok(())
    }

    fn collect_input(&mut self, p: &PathDescriptor) -> Result<()> {
        debug!("collecting input: {:?}", p.get_file_name().to_string());
        self.nodes_to_insert
            .push(NodeToInsert::InputFile(p.clone(), None));
        Ok(())
    }

    pub(crate) fn add_tupfile(&mut self, p0: &PathDescriptor) -> Result<()> {
        if self.processed.insert(p0.clone()) {
            self.collect_input(p0)?;
        }
        Ok(())
    }

    fn collect_rule(&mut self, p: &RuleDescriptor, _hash: Option<String>) {
        self.nodes_to_insert.push(NodeToInsert::Rule(p.clone()));
    }
    fn collect_task(&mut self, p: &TaskDescriptor, _hash: Option<String>) {
        self.nodes_to_insert.push(NodeToInsert::Task(p.clone()));
    }
    fn add_env(&mut self, p: &EnvDescriptor) {
        if self.processed_envs.insert(p.clone()) {
            self.nodes_to_insert.push(NodeToInsert::Env(p.clone()));
        }
    }

    fn add_input_for_insert(
        &mut self,
        resolved_input: &InputResolvedType,
        id: TupPathDescriptor,
    ) -> Result<()> {
        if let Some(p) = resolved_input.get_glob_path_desc() {
            if self.processed_globs.insert(p.clone()) {
                self.collect_glob(&p, id)?;
            }
        }
        if let Some(p) = resolved_input.get_resolved_path_desc() {
            if self.processed.insert(p.clone()) {
                self.collect_input(&p)?;
            }
        }
        self.add_group(resolved_input.get_group_input());
        Ok(())
    }

    fn add_excluded(&mut self, p: PathDescriptor) -> Result<()> {
        if self.processed.insert(p.clone()) {
            self.collect_excluded(&p)?;
        }
        Ok(())
    }
    fn add_task_node(&mut self, task_desc: &TaskDescriptor, hash: Option<String>) -> Result<()> {
        let task_instance = self.read_write_buffer_objects.get_task(task_desc);
        let name = task_instance.get_target();
        let tuppath = task_instance.get_parent();
        let tuppathstr = tuppath.as_path();
        //let tupid = task_instance.get_tupfile_desc();
        let line = task_instance.get_task_loc();
        debug!(
            " task to insert: {} at  {} at {}",
            name,
            tuppathstr.display(),
            line
        );
        let prevline = self
            .unique_rule_check
            .insert(task_instance.get_path().to_string(), line.clone());
        if prevline.is_none() {
            self.collect_task(task_desc, hash);
        } else {
            bail!(
                "Task at  {}:{} was previously defined at line {}. \
                Ensure that rule definitions take the inputs as arguments.",
                tuppathstr.display(),
                line,
                prevline.unwrap()
            );
        }
        Ok(())
    }
    fn add_rule_node(&mut self, rule_desc: &RuleDescriptor, hash: Option<String>) -> Result<()> {
        let rule_formula = self.read_write_buffer_objects.get_rule(rule_desc);
        let name = rule_formula.get_rule_str();
        let ref ph = self.read_write_buffer_objects;
        let tup_cwd_desc = &rule_formula.get_rule_ref().get_tup_dir();
        let tuppath = ph.get_path(&tup_cwd_desc);
        //let tupid = rule_formula.get_rule_ref().get_tupfile_desc();
        {
            let rule_ref = rule_formula.get_rule_ref();
            if log::log_enabled!(log::Level::Debug) {
                let tuppathstr = tuppath.as_path();
                debug!(
                    " rule to insert: {} at  {}:{}",
                    name,
                    tuppathstr.display(),
                    rule_ref
                );
            }
            self.collect_rule(rule_desc, hash);
        }
        Ok(())
    }
    pub(crate) fn add_group(&mut self, p0: Option<GroupPathDescriptor>) {
        if let Some(group) = p0 {
            if self.processed_groups.insert(group.clone()) {
                self.nodes_to_insert.push(NodeToInsert::Group(group));
            }
        }
    }
}

/// nodes to insert after rules have been resolved
fn insert_nodes(
    tx: &mut TupTransaction,
    read_write_buf: &ReadWriteBufferObjects,
    crossref: &mut CrossRefMaps,
    term_progress: &TermProgress,
    pb: &ProgressBar,
    insert_label: &str,
    nodes: &[NodeToInsert],
    envs_to_insert: &HashSet<EnvDescriptor>,
    //check_cancel: F,
) -> Result<()> {
    let total_steps = nodes.len() + envs_to_insert.len();
    let il = insert_label.to_string();
    {
        let len = std::cmp::max(1, total_steps as u64);
        pb.set_length(len);
        pb.set_position(0);
        pb.set_message(Cow::Owned(il));
    }

    let parent_descriptors = nodes
        .iter()
        .map(|n| (n.get_parent_id(read_write_buf), n.get_type()));
    //check_cancel()?;

    for (parent_desc, rowtype) in parent_descriptors {
        if parent_desc.is_root() || crossref.get_path_db_id(&parent_desc).is_some() {
            // already inserted
            continue;
        }
        let (parid, parparid) =
            ensure_parent_inserted(tx, read_write_buf.get(), crossref, &rowtype, &parent_desc)?;
        if !parid.is_negative() {
            crossref.add_path_xref(parent_desc, parid, parparid);
        }
    }
    let node_cnt = nodes.len();
    let sqrt_node_cnt = (node_cnt as f64).sqrt() as usize + 1;
    for (i, node_to_insert) in nodes.iter().enumerate() {
        if i % sqrt_node_cnt == 0 {
            log::info!("Inserting node {}/{} ", i + 1, nodes.len());
            check_cancel()?;
        }
        debug!("inserting node: {:?}", node_to_insert);
        // for an output node ensure that srcid is unique and does not conflict with existing rules
        insert_node(tx, &read_write_buf, crossref, node_to_insert)
            .wrap_err(format!("Inserting node :{:?}", node_to_insert))?;
        {
            term_progress.update_pb(&pb);
        }
    }

    for env_var in envs_to_insert.iter() {
        let env_val = env_var.get_val_str();
        let key = env_var.get_key_str();
        match tx.upsert_env_var(key, env_val) {
            Ok(ups) => {
                let env_id = ups.get_id();
                crossref.add_env_xref(env_var.clone(), env_id);
                if !ups.is_unchanged() {
                    tx.mark_modified(env_id, &Env)?;
                }
                tx.mark_present(env_id, &Env)?;
            }
            Err(e) => {
                log::warn!("Error while inserting env var: {:?}", e);
            }
        }
        term_progress.update_pb(pb);
    }
    check_cancel()?;

    pb.set_message(format!("✔ Done {}", insert_label));

    if total_steps > 0 {
        pb.set_position(total_steps as u64);
    }

    Ok(())
}

fn collect_nodes_to_insert<F>(
    read_write_buf: &ReadWriteBufferObjects,
    resolved_rules: &ResolvedRules,
    check_cancel: F,
) -> Result<(HashSet<EnvDescriptor>, Vec<NodeToInsert>), Report>
where
    F: Fn() -> Result<(), Report>,
{
    let mut envs_to_insert = HashSet::new();

    // collect all un-added groups and add them in a single transaction.
    let mut nodes: Vec<_> = {
        let mut collector = Collector::new(read_write_buf.clone())?;
        for resolvedtasks in resolved_rules.tasks_by_tup().iter() {
            let tuploc = resolvedtasks.first().map(|x| x.get_task_loc());
            for resolvedtask in resolvedtasks.iter() {
                let tupid = tuploc.unwrap().get_tupfile_desc();
                let rd = resolvedtask.get_task_descriptor();
                for s in resolvedtask.get_deps() {
                    collector.add_input_for_insert(s, tupid.clone())?;
                }
                collector.add_task_node(rd, None)?;
                let env_desc = resolvedtask.get_env_list();
                envs_to_insert.extend(env_desc.iter());
            }
        }

        for tupfile in resolved_rules.get_tupfiles_read() {
            collector.add_tupfile(tupfile)?;
        }
        for rl in resolved_rules.get_resolved_links().iter() {
            let rd = rl.get_rule_desc();
            let rule_ref = rl.get_rule_ref();
            let tup_desc = rule_ref.get_tupfile_desc();
            //let dir = get_dir(&tup_desc, &crossref)?;
            collector.add_rule_node(rd, None)?;
            for p in rl.get_targets() {
                collector.add_output(p, SrcId::RuleId(rl.get_rule_desc().clone()))?
            }
            let group = rl.get_group_desc();
            collector.add_group(group.cloned());

            for s in rl.get_sources() {
                collector.add_input_for_insert(s, tup_desc.clone())?;
            }
            for p in rl.get_excluded_targets() {
                collector.add_excluded(p.clone())?
            }
            for env in rl.get_env_list().iter() {
                collector.add_env(&env);
            }
            check_cancel()?;
        }
        collector.nodes()
    };
    // sort nodes by type and then by path for better performance (as parent directories are inserted first)
    nodes.sort_by(|a, b| {
        if a.get_type().eq(&b.get_type()) {
            let i = a.get_depth(read_write_buf);
            let j = b.get_depth(read_write_buf);
            if i == j {
                let pa = a.get_parent_id(read_write_buf);
                let pb = b.get_parent_id(read_write_buf);
                pa.dict_less(&pb)
            } else {
                i.cmp(&j)
            }
        } else {
            a.get_type().cmp(&b.get_type())
        }
    });
    Ok((envs_to_insert, nodes))
}

fn insert_node(
    tx: &mut TupTransaction,
    read_write_buf: &ReadWriteBufferObjects,
    crossref: &mut CrossRefMaps,
    node_to_insert: &NodeToInsert,
) -> Result<(), Report> {
    let node = node_to_insert.get_node(&read_write_buf, crossref)?;
    // Cache the sha so we don't recompute it on modified existing nodes.
    let compute_sha = || {
        let sha = if matches!(node_to_insert.get_row_type(), Rule | Task) {
            String::new()
        } else {
            node_to_insert.get_saved_hash().unwrap_or_default()
        };
        sha
    };
    let sha = compute_sha(); // need to compute sha before upsert for new nodes and modified nodes
    let (db_id, db_par_id) = {
        let upsnode = tx.fetch_upsert_node_raw(&node, || sha.clone())?;
        (upsnode.get_id(), upsnode.get_dir())
    };
    if !db_id.is_negative() {
        node_to_insert.update_crossref(crossref, db_id, db_par_id);
        {
            if !sha.is_empty() && !matches!(node_to_insert.get_row_type(), Rule | Task) {
                tx.upsert_node_sha(db_id, &sha)?;
            }
        }
    } else {
        log::warn!("Failed to insert node: {:?}", node);
    }
    Ok(())
}

pub(crate) struct ConnWrapper<'a, 'b> {
    conn: &'b TupConnectionRef<'a>,
}

impl<'a, 'b> ConnWrapper<'a, 'b> {
    pub fn new(conn: &'b TupConnectionRef<'a>) -> Self {
        Self { conn }
    }
}

impl<'a, 'b> tupparser::decode::GroupInputs for ConnWrapper<'a, 'b> {
    fn get_group_paths(&self, group_name: &str, rule_id: i64, rule_dir: i64) -> Option<String>
    where
        Self: Sized,
    {
        // first fetch all the input groups to the given rule
        let group_name = if group_name.starts_with("<") {
            group_name.to_string()
        } else {
            format!("<{}>", group_name)
        };

        let rule_dir = self.conn.fetch_dirpath(rule_dir).expect(&*format!(
            "failed to fetch dir path for rule:{} in dir :{}",
            rule_id, rule_dir
        ));

        let grp_id_maybe = self
            .conn
            .fetch_rule_input_matching_group_name(rule_id, group_name.as_str())
            .ok()
            .flatten();

        grp_id_maybe.and_then(|grp_id| -> Option<String> {
            let mut path = String::new();
            self.conn
                .for_each_group_inputs(grp_id, |n| {
                    let p: &str = n.get_name();
                    path.push_str(
                        pathdiff::diff_paths(p, rule_dir.as_path())
                            .unwrap_or(PathBuf::from(p))
                            .to_string_lossy()
                            .to_string()
                            .as_str(),
                    );
                    path.push_str(" ");
                    path.pop();
                    Ok(())
                })
                .ok()?;
            Some(path)
        })
    }
}

fn is_generated(p0: &RowType) -> bool {
    p0 == &GenF || p0 == &DirGen
}

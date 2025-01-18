use crate::scan::{HashedPath, MAX_THRS_DIRS};
use crate::TermProgress;
use bimap::BiMap;
use crossbeam::sync::WaitGroup;
use eyre::{bail, eyre, OptionExt, Report, Result};
use log::debug;
use parking_lot::Mutex;
use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tupdb::db::RowType::{Dir, DirGen, Env, Excluded, File, GenF, Glob, Rule, Task, TupF};
use tupdb::db::{
    create_path_buf_temptable, db_path_str, MiscStatements, Node, RowType, TupConnection,
    TupConnectionRef,
};
use tupdb::deletes::LibSqlDeletes;
use tupdb::error::{AnyError, DbResult};
use tupdb::inserts::LibSqlInserts;
use tupdb::queries::LibSqlQueries;
use tupparser::buffers::{EnvDescriptor, GlobPathDescriptor, GroupPathDescriptor, OutputHolder, PathBuffers, PathDescriptor, RuleDescriptor, RuleRefDescriptor, TaskDescriptor, TupPathDescriptor};
use tupparser::decode::{OutputHandler, PathDiscovery, PathSearcher};
use tupparser::errors::Error;
use tupparser::paths::{GlobPath, InputResolvedType, MatchingPath, NormalPath, SelOptions};
use tupparser::transform::{compute_dir_sha256, compute_sha256};
use tupparser::{ReadWriteBufferObjects, ResolvedRules, TupParser};
use RowType::Group;

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
    GeneratedFile(PathDescriptor, SrcId),
    Env(EnvDescriptor),
    Task(TaskDescriptor),
    #[allow(dead_code)]
    InputFile(PathDescriptor),
    Glob(GlobPathDescriptor, TupPathDescriptor),
    ExcludedFile(PathDescriptor),
    Dir(PathDescriptor),
    DirGen(PathDescriptor),
}

impl PathDiscovery for ConnWrapper<'_, '_> {
    fn discover_paths_with_cb(
        &self,
        path_buffers: &impl PathBuffers,
        glob_path: &[GlobPath],
        cb: impl FnMut(MatchingPath),
        sel: SelOptions,
    ) -> std::result::Result<usize, Error> {
        let sz = DbPathSearcher::fetch_glob_globs_cb(&self.conn, path_buffers, glob_path, cb, sel)
            .map_err(|e| Error::new_callback_error(e.to_string()))?;
        Ok(sz)
    }
}

impl NodeToInsert {
    #[allow(dead_code)]
    pub(crate) fn new(bo: &impl PathBuffers, p0: &Node, full_path: &Path) -> Option<Self> {
        let p = bo.add_abs(full_path).unwrap();

        match p0.get_type() {
            File => Some(NodeToInsert::InputFile(p)),
            Dir => Some(NodeToInsert::Dir(p)),
            GenF => Some(NodeToInsert::GeneratedFile(
                p,
                SrcId::RuleId(Default::default()),
            )),
            TupF => Some(NodeToInsert::Tup(p)),
            DirGen => Some(NodeToInsert::DirGen(p)),
            Glob => Some(NodeToInsert::Glob(p, Default::default())),
            _ => None,
        }
    }

    pub fn get_name(&self, read_write_buffer_objects: &ReadWriteBufferObjects) -> String {
        match self {
            NodeToInsert::Rule(r) => read_write_buffer_objects.get_rule(r).get_rule_str(),
            NodeToInsert::Tup(t) => read_write_buffer_objects
                .get_tup_path(t)
                .file_name()
                .to_string(),
            NodeToInsert::Group(g) => read_write_buffer_objects.get_group_name(g),
            NodeToInsert::GeneratedFile(p, _) => read_write_buffer_objects
                .get_path(p)
                .file_name()
                .to_string(),
            NodeToInsert::Env(e) => read_write_buffer_objects.get_env_name(e),
            NodeToInsert::Task(t) => read_write_buffer_objects.get_task(t).to_string(),
            NodeToInsert::InputFile(p) => read_write_buffer_objects
                .get_path(p)
                .file_name()
                .to_string(),
            NodeToInsert::Glob(g, _) => read_write_buffer_objects.get_name(g),
            NodeToInsert::ExcludedFile(e) => read_write_buffer_objects.get_name(e),
            NodeToInsert::Dir(p) => read_write_buffer_objects.get_name(p),
            NodeToInsert::DirGen(p) => read_write_buffer_objects.get_name(p),
        }
    }

    #[allow(dead_code)]
    fn get_input_file(&self) -> Option<PathDescriptor> {
        match self {
            NodeToInsert::InputFile(p) => Some(p.clone()),
            _ => None,
        }
    }

    pub fn get_id(&self) -> i64 {
        match self {
            NodeToInsert::Rule(r) => r.to_u64() as i64,
            NodeToInsert::Tup(t) => t.to_u64() as i64,
            NodeToInsert::Group(g) => g.to_u64() as i64,
            NodeToInsert::GeneratedFile(p, _) => p.to_u64() as i64,
            NodeToInsert::Env(e) => e.to_u64() as i64,
            NodeToInsert::Task(t) => t.to_u64() as i64,
            NodeToInsert::InputFile(p) => p.to_u64() as i64,
            NodeToInsert::Glob(g, _) => g.to_u64() as i64,
            NodeToInsert::ExcludedFile(p) => p.to_u64() as i64,
            NodeToInsert::Dir(p) => p.to_u64() as i64,
            NodeToInsert::DirGen(p) => p.to_u64() as i64,
        }
    }

    pub fn get_row_type(&self) -> RowType {
        match self {
            NodeToInsert::Rule(_) => Rule,
            NodeToInsert::Tup(_) => TupF,
            NodeToInsert::Group(_) => Group,
            NodeToInsert::GeneratedFile(_, _) => GenF,
            NodeToInsert::Env(_) => Env,
            NodeToInsert::Task(_) => Task,
            NodeToInsert::InputFile(_) => File,
            NodeToInsert::Glob(_, _) => Glob,
            NodeToInsert::ExcludedFile(_) => Excluded,
            NodeToInsert::Dir(_) => Dir,
            NodeToInsert::DirGen(_) => DirGen,
        }
    }

    pub fn get_srcid(&self, cross_ref_maps: &CrossRefMaps) -> Result<i64> {
        match self {
            NodeToInsert::GeneratedFile(gen, SrcId::RuleId(id)) => cross_ref_maps
                .get_rule_db_id(id)
                .map(|x| x.0)
                .ok_or_eyre(eyre!(
                    "srcid not found for generated file: {:?} with rule descriptor:{:?}",
                    gen,
                    id
                )),

            NodeToInsert::Glob(g, tupid) => cross_ref_maps
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
            NodeToInsert::Glob(g, _) => read_write_buffer_objects.get_recursive_glob_str(g),
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
            NodeToInsert::GeneratedFile(p, _) => read_write_buffer_objects.get_parent_id(p),
            NodeToInsert::Env(_) => PathDescriptor::default(),
            NodeToInsert::Task(t) => read_write_buffer_objects.get_task(t).get_parent_id(),
            NodeToInsert::InputFile(p) => read_write_buffer_objects.get_parent_id(p),
            NodeToInsert::Glob(g, _) => read_write_buffer_objects.get_glob_prefix(g),
            NodeToInsert::ExcludedFile(e) => read_write_buffer_objects.get_parent_id(e),
            NodeToInsert::Dir(p) => read_write_buffer_objects.get_parent_id(p),
            NodeToInsert::DirGen(p) => read_write_buffer_objects.get_parent_id(p),
        }
    }

    pub fn compute_node_sha(
        &self,
        conn: &TupConnectionRef,
        path_buffers: &impl PathBuffers,
    ) -> Option<String> {
        let compute_sha_for = |p: &PathDescriptor| {
            let p = PathBuffers::get_abs_path(path_buffers, p);
            let sha = compute_sha256(p.as_path()).ok();
            debug!("sha for {:?} is {:?}", p, sha);
            sha
        };
        let compute_sha_for_glob = |p: &PathDescriptor| {
            let sha =
                tupparser::transform::compute_glob_sha256(&ConnWrapper::new(conn), path_buffers, p)
                    .ok();
            debug!("sha for {:?} is {:?}", p, sha);
            sha
        };

        let compute_sha_for_dir = |p: &GlobPathDescriptor| {
            let p = path_buffers.get_abs_path(p);
            let sha = compute_dir_sha256(p.as_path()).ok();
            debug!("sha for {:?} is {:?}", p, sha);
            sha
        };
        match self {
            NodeToInsert::GeneratedFile(p, _) => compute_sha_for(p),
            NodeToInsert::Tup(p) => compute_sha_for(p),
            NodeToInsert::InputFile(p) => compute_sha_for(p),
            NodeToInsert::Glob(g, _) => compute_sha_for_glob(g),
            NodeToInsert::DirGen(p) => compute_sha_for_dir(p),
            NodeToInsert::Dir(p) => compute_sha_for_dir(p),
            _ => None,
        }
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
                0,
            ),
            TupF | File | GenF | Glob => {
                Node::new_file_or_genf(
                    self.get_id(),
                    parent_id,
                    0,
                    self.get_name(read_write_buffer_objects),
                    self.get_row_type(),
                    self.get_srcid(crossref)?, // at this point we know that rules have been inserted it is generated
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
                0,
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
            NodeToInsert::GeneratedFile(p, _) => {
                crossref.add_path_xref(p.clone(), id, parid);
            }
            NodeToInsert::Env(e) => {
                crossref.add_env_xref(e.clone(), id);
            }
            NodeToInsert::Task(t) => {
                crossref.add_task_xref(t.clone(), id, parid);
            }
            NodeToInsert::InputFile(p) => {
                crossref.add_path_xref(p.clone(), id, parid);
            }
            NodeToInsert::Glob(g, _) => {
                crossref.add_path_xref(g.clone(), id, parid);
            }
            NodeToInsert::ExcludedFile(p) => {
                crossref.add_path_xref(p.clone(), id, parid);
            }
            NodeToInsert::Dir(d) => {
                crossref.add_path_xref(d.clone(), id, parid);
            }
            NodeToInsert::DirGen(d) => {
                crossref.add_path_xref(d.clone(), id, parid);
            }
        }
    }
    pub fn get_path(&self, read_write_buffer_objects: &ReadWriteBufferObjects) -> String {
        self.get_parent_id(read_write_buffer_objects)
            .get_path_ref()
            .to_string()
            + "/"
            + &*self.get_name(read_write_buffer_objects)
    }
    pub fn get_type(&self) -> RowType {
        self.get_row_type()
    }
}
impl CrossRefMaps {
    pub fn get_group_db_id(&self, g: &GroupPathDescriptor) -> Option<(i64, i64)> {
        self.group_id.get_by_left(g).copied()
    }
    pub fn get_group_db_id_ok(&self, g: &GroupPathDescriptor) -> Result<(i64, i64)> {
        self.group_id.get_by_left(g).copied().ok_or_else(|| eyre!("group {} not found in crossref", g))
    }
    
    pub fn get_path_db_id(&self, p: &PathDescriptor) -> Option<(i64, i64)> {
        if p.eq(&PathDescriptor::default()) {
            Some((1, 0))
        } else {
            self.path_id.get_by_left(p).copied()
        }
    }
    pub fn get_path_db_id_ok(&self, p: &PathDescriptor) -> Result<(i64, i64)> {
        self.get_path_db_id(p).ok_or_else(|| eyre!("path {} not found in crossref", p))
    }
    pub fn get_rule_db_id(&self, r: &RuleDescriptor) -> Option<(i64, i64)> {
        self.rule_id.get_by_left(r).copied()
    }
    
    pub fn get_rule_db_id_ok(&self, r: &RuleDescriptor) -> Result<(i64, i64)> {
        self.get_rule_db_id(r).ok_or_else(|| eyre!("rule {} not found in crossref", r))
    }

    pub fn get_tup_db_id(&self, r: &TupPathDescriptor) -> Option<(i64, i64)> {
        self.tup_id.get_by_left(r).copied()
    }

    pub fn get_tup_db_id_ok(&self, r: &TupPathDescriptor) -> Result<(i64, i64)> {
        self.get_tup_db_id(r).ok_or_else(|| eyre!("tup {} not found in crossref", r))
    }
    pub fn get_env_db_id(&self, e: &EnvDescriptor) -> Option<i64> {
        self.env_id.get_by_left(e).copied()
    }

    pub fn get_env_db_id_ok(&self, e: &EnvDescriptor) -> Result<i64> {
        self.get_env_db_id(e).ok_or_else(|| eyre!("env {} not found in crossref", e))
    }
    pub fn get_glob_db_id(&self, s: &GlobPathDescriptor) -> Option<(i64, i64)> {
        self.path_id.get_by_left(&s).copied()
    }
    pub fn get_glob_db_id_ok(&self, s: &GlobPathDescriptor) -> Result<(i64, i64)> {
        self.get_glob_db_id(s).ok_or_else(|| eyre!("glob {} not found in crossref", s))
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

fn into_any_error(e: Error) -> AnyError {
    AnyError::from(e.to_string())
}
/// Path searcher that scans the sqlite database for matching paths
/// It is used by the parser to resolve paths and globs. Resolved outputs are dumped in OutputHolder
#[derive(Clone)]
struct DbPathSearcher {
    conn: Arc<Mutex<TupConnection>>,
    psx: OutputHolder,
    root: PathBuf,
}

impl DbPathSearcher {
    pub fn new<P: AsRef<Path>>(conn: Arc<Mutex<TupConnection>>, root: P) -> DbPathSearcher {
        DbPathSearcher {
            conn,
            psx: OutputHolder::new(),
            root: root.as_ref().to_path_buf(),
        }
    }

    pub fn update_delete_list(&self) -> Result<()> {
        let conn = self.conn.lock();
        conn.deref().delete_tupentries_in_deleted_tupfiles()?;
        Ok(())
    }

    pub fn fetch_glob_globs_cb<F>(
        conn: &TupConnectionRef,
        ph: &impl PathBuffers,
        glob_paths: &[GlobPath],
        ref mut f: F,
        sel: SelOptions,
    ) -> Result<usize, AnyError>
    where
        F: FnMut(MatchingPath),
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
                let full_path_pd = ph
                    .add_abs(s)
                    .map_err(into_any_error)?;
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
                    f(mp);
                }
                Ok(())
            };

            conn.fetch_glob_matches(
                non_pattern_path.get_path_ref().as_path(),
                &glob_pattern.to_string(),
                fetch_row,
                2,
            )?;
            return Ok(num_matches);
        }
        log::warn!("no rows found for any glob pattern: {:?}", glob_paths);
        Err(AnyError::query_returned_no_rows())
    }
}
impl Drop for DbPathSearcher {
    fn drop(&mut self) {
        debug!("dropping DbPathSearcher");
        let mutconn = self.conn.lock();
        let _ = mutconn.deref().drop_tupfile_entries_table();
    }
}
impl PathDiscovery for DbPathSearcher {
    fn discover_paths_with_cb(
        &self,
        path_buffers: &impl PathBuffers,
        glob_paths: &[GlobPath],
        mut cb: impl FnMut(MatchingPath),
        sel: SelOptions,
    ) -> Result<usize, Error> {
        let conn = self.conn.deref().lock();
        let tup_connection_ref = conn.as_ref();
        let mut match_count =
            Self::fetch_glob_globs_cb(&tup_connection_ref, path_buffers, glob_paths, &mut cb, sel)
                .map_err(|e| Error::new_callback_error(e.to_string()))?;
        if match_count == 0 {
            let mps = self
                .get_outs()
                .discover_paths(path_buffers, glob_paths)
                .map_err(|e| Error::new_path_search_error(e.to_string()))?;
            for mp in mps {
                cb(mp);
                match_count += 1;
            }
        }
        Ok(match_count)
    }

    fn discover_paths(
        &self,
        path_buffers: &impl PathBuffers,
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
            },
            sel,
        )?;
        let c = |x: &MatchingPath, y: &MatchingPath| {
            let x = x.path_descriptor();
            let y = y.path_descriptor();
            x.cmp(&y)
        };
        mps.sort_by(c);
        mps.dedup();
        Ok(mps)
    }
}
impl PathSearcher for DbPathSearcher {
    fn locate_tuprules(
        &self,
        tup_cwd: &PathDescriptor,
        path_buffers: &impl PathBuffers,
    ) -> Vec<PathDescriptor> {
        let conn = self.conn.lock();
        //let mut node_path_stmt = conn.deref().fetch_node_path_prepare().unwrap();
        let tup_path = tup_cwd.get_path_ref();
        debug!(
            "tup path is : {} in which (or its parents) we look for TupRules.tup or Tuprules.lua",
            tup_path.as_path().display()
        );
        let dirid = conn
            .deref()
            .fetch_dir_from_path(tup_path.as_path())
            .unwrap_or(1i64);
        debug!("dirid: {}", dirid);
        let mut tup_rules = Vec::new();
        let tuprs = ["TupRules.tup", "tuprules.lua"];
        let mut add_rules = |dirid| {
            for tupr in tuprs {
                if let Ok((dirid_, name)) = conn
                    .fetch_closest_parent(tupr, dirid)
                    .inspect_err(|e| eprintln!("Error while looking for tuprules: {}", e))
                {
                    debug!("tup rules node  : {} dir:{}", name, dirid_);
                    let node_dir = conn.fetch_dirpath(dirid_)
                        .unwrap_or_else(|e|
                            panic!("Directory {dirid_} not found while trying to locate tuprules. Error {e}")
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

    fn get_outs(&self) -> &OutputHolder {
        &self.psx
    }

    fn get_root(&self) -> &Path {
        self.root.as_path()
    }

    fn merge(&mut self, p: &impl PathBuffers, o: &impl OutputHandler) -> Result<(), Error> {
        OutputHandler::merge(&mut self.psx, p, o)
    }
}

pub(crate) fn parse_tupfiles_in_db_for_dump<P: AsRef<Path>>(
    connection: TupConnection,
    tupfiles: Vec<Node>,
    root: P,
    term_progress: &TermProgress,
    var: &String,
) -> Result<Vec<(TupPathDescriptor, String)>> {
    {
        let conn = Arc::from(Mutex::new(connection));
        let db = DbPathSearcher::new(conn.clone(), root.as_ref());
        let mut parser = TupParser::try_new_from(root.as_ref(), db)?;
        let pb = term_progress.make_progress_bar("_");
        let mut states_after_parse = Vec::new();
        {
            for tupfile in tupfiles
                .iter()
                .filter(|x| !x.get_name().ends_with(".lua"))
                .cloned()
            {
                pb.set_message(format!("Parsing :{}", tupfile.get_name()));
                let statements_to_resolve = parser.parse_tupfile_immediate(tupfile.get_name())?;
                if let Some(val) = statements_to_resolve.fetch_var(var) {
                    states_after_parse
                        .push((statements_to_resolve.get_cur_file_desc().clone(), val));
                }
                pb.set_message(format!("Done parsing {}", tupfile.get_name()));
                term_progress.tick(&pb);
            }
        }
        Ok(states_after_parse)
    }
}
/// handle the tup parse command which assumes files in db and adds rules and makes links joining input and output to/from rule statements
pub(crate) fn parse_tupfiles_in_db<P: AsRef<Path>>(
    connection: TupConnection,
    tupfiles: Vec<Node>,
    root: P,
    term_progress: &TermProgress,
) -> Result<()> {
    let conn = Arc::from(Mutex::new(connection));
    {
        let conn = conn.deref().lock();
        conn.create_tupfile_entries_table()?;
    }
    //conn.prune_modified_list_basic()?;
    let db = DbPathSearcher::new(conn, root.as_ref());
    let parser = TupParser::try_new_from(root.as_ref(), db)?;
    {
        let dbref = parser.get_mut_searcher();
        let conn = dbref.conn.deref().lock();
        let tupfile_ids = tupfiles.iter().map(|t| t.get_id()).collect::<Vec<_>>();
        conn.enrich_modified_list()?;
        conn.mark_tupfile_entries(&tupfile_ids)?;
    }
    parse_and_add_rules_to_db(parser, tupfiles.as_slice(), &term_progress)?;

    Ok(())
}


fn insert_links(
    conn: &mut TupConnection,
    resolved_rules: &ResolvedRules,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    // collect all un-added groups and add them in a single transaction.
    let mut link_collector = LinkCollector::new();
    let links = {
        for tupfile_read in resolved_rules.get_tupfiles_read() {
            link_collector.add_tupfile_to_tupfile_link(tupfile_read, resolved_rules.get_tupid());
        }
        add_links_from_to_globs(conn, resolved_rules, crossref, &mut link_collector)?;
        add_links_from_to_rules_and_groups(resolved_rules, &mut link_collector);
        link_collector.links()
    };
    let tx = conn.transaction()?;
    {
        for l in links {
            match l {
                Link::InputToRule(i, rd) => {
                    let (input_id, _) = crossref.get_path_db_id(&i).ok_or_else(|| {
                        eyre!("input not found:{:?} mentioned in rule {:?}", i, rd)
                    })?;
                    let (rule_id, _) = crossref.get_rule_db_id(&rd).ok_or_else(|| {
                        eyre!("rule not found:{:?} mentioned in rule {:?}", rd, i)
                    })?;
                    tx.insert_link(input_id, rule_id, false.into(), Rule)
                        .map_err(|e| eyre!(e.to_string()))?;
                }
                Link::RuleToOutput(rd, out) => {
                    let (rule_id, _) = crossref.get_rule_db_id(&rd).ok_or_else(|| {
                        eyre!("rule not found:{:?} mentioned in output {:?}", rd, out)
                    })?;
                    let (output_id, _) = crossref.get_path_db_id(&out).ok_or_else(|| {
                        eyre!("output not found:{:?} mentioned in rule {:?}", out, rd)
                    })?;
                    tx.insert_link(output_id, rule_id, true.into(), Rule)
                        .map_err(|e| eyre!(e.to_string()))?;
                }
                Link::OutputToGroup(out, group) => {
                    let (output_id, _) = crossref.get_path_db_id(&out).ok_or_else(|| {
                        eyre!("output not found:{:?} mentioned in group {:?}", out, group)
                    })?;
                    let (group_id, _) = crossref.get_group_db_id(&group).ok_or_else(|| {
                        eyre!("group not found:{:?} mentioned in output {:?}", group, out)
                    })?;
                    tx.insert_link(output_id, group_id, true.into(), Group)?;
                }
                Link::GroupToRule(group, rd) => {
                    let (group_id, _) = crossref.get_group_db_id(&group).ok_or_else(|| {
                        eyre!("group not found:{:?} mentioned in rule {:?}", group, rd)
                    })?;
                    let (rule_id, _) = crossref.get_rule_db_id(&rd).ok_or_else(|| {
                        eyre!("rule not found:{:?} mentioned in group {:?}", rd, group)
                    })?;
                    tx.insert_link(group_id, rule_id, false.into(), Rule)?;
                }
                Link::EnvToRule(env, rd) => {
                    let env_id = crossref.get_env_db_id(&env).ok_or_else(|| {
                        eyre!("env not found:{:?} mentioned in rule {:?}", env, rd)
                    })?;
                    let (rule_id, _) = crossref.get_rule_db_id(&rd).ok_or_else(|| {
                        eyre!("rule not found:{:?} mentioned in env {:?}", rd, env)
                    })?;
                    tx.insert_link(env_id, rule_id, true.into(), Rule)?;
                }
                Link::TupfileToTupfile(pd, tupd) => {
                    let (tupfile_id, _) = crossref.get_tup_db_id(&tupd).ok_or_else(|| {
                        eyre!("tupfile not found:{:?} mentioned in tupfile {:?}", tupd, pd)
                    })?;
                    let (tupfile_read_id, _) = crossref.get_path_db_id(&pd).ok_or_else(|| {
                        eyre!("tupfile not found:{:?} mentioned in tupfile {:?}", pd, tupd)
                    })?;
                    tx.insert_link(tupfile_read_id, tupfile_id, true.into(), TupF)?;
                }
                Link::GlobToTupfile(g, tupd) => {
                    let (glob_id, _) = crossref.get_glob_db_id(&g).ok_or_else(|| {
                        eyre!("glob not found:{:?} mentioned in tupfile {:?}", g, tupd)
                    })?;
                    let (tupfile_id, _) = crossref.get_tup_db_id(&tupd).ok_or_else(|| {
                        eyre!("tupfile not found:{:?} mentioned in glob {:?}", tupd, g)
                    })?;
                    tx.insert_link(glob_id, tupfile_id, true.into(), TupF)?;
                }
                Link::TupfileToRule(tupd, rd) => {
                    let (tupfile_id, _) = crossref.get_tup_db_id(&tupd).ok_or_else(|| {
                        eyre!("tupfile not found:{:?} mentioned in rule {:?}", tupd, rd)
                    })?;
                    let (rule_id, _) = crossref.get_rule_db_id(&rd).ok_or_else(|| {
                        eyre!("rule not found:{:?} mentioned in tupfile {:?}", rd, tupd)
                    })?;
                    tx.insert_link(tupfile_id, rule_id, false.into(), Rule)?;
                }
                Link::DirToGlob(dir, glob) => {
                    let (dir_id, _) = crossref.get_path_db_id(&dir).ok_or_else(|| {
                        eyre!("dir not found:{:?} mentioned in glob {:?}", dir, glob)
                    })?;
                    let (glob_id, _) = crossref.get_glob_db_id(&glob).ok_or_else(|| {
                        eyre!("glob not found:{:?} mentioned in dir {:?}", glob, dir)
                    })?;
                    tx.insert_link(dir_id, glob_id, true.into(), Glob)?;
                }
            }
        }
    }
    tx.commit()?;
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

fn add_links_from_to_globs(
    conn: &mut TupConnection,
    resolved_rules: &ResolvedRules,
    crossref: &mut CrossRefMaps,
    link_collector: &mut LinkCollector,
) -> Result<(), Report> {
    for glob_desc in resolved_rules.get_globs_read() {
        let (_, glob_dir) = crossref.get_glob_db_id(glob_desc).ok_or_else(|| {
            eyre!(
                "glob not found:{:?} mentioned in tupfile {:?}",
                glob_desc,
                resolved_rules.get_tupid()
            )
        })?;
        link_collector.add_globs_read_to_tupfile_link(glob_desc, resolved_rules.get_tupid());
        let glob_path = GlobPath::build_from(&PathDescriptor::default(), glob_desc)?;
        let non_pattern_path = glob_path.get_non_pattern_prefix_desc();
        let depth = glob_desc.components().count() - non_pattern_path.components().count();
        if depth > 1 {
            conn.for_each_glob_dir(glob_dir, depth as i32, |dir| -> DbResult<()> {
                let tup_cwd = PathDescriptor::default();
                let dir_path = tup_cwd.join(dir).map_err(into_any_error)?;
                link_collector.add_dir_to_glob_link(&dir_path, resolved_rules.get_tupid());
                Ok(())
            })?;
        } else {
            link_collector.add_dir_to_glob_link(glob_desc, resolved_rules.get_tupid());
        }
    }
    Ok(())
}

/// adds links from glob patterns specified at each directory that are inputs to rules  to the tupfile directory
/// We don't directly add links from glob patterns to rules but instead add links from glob patterns to the tupfile so that they are parsed whenever glob patterns are modified
/// Newer / modified /deleted inputs discovered in glob patterns and added as rule inputs will be process in a re-iterations parsing phase of Tupfile which the glob pattern links to
#[allow(dead_code)]
fn add_link_glob_dir_to_rules(
    conn: &mut TupConnection,
    rw_buf: &ReadWriteBufferObjects,
    resolved_rules: &ResolvedRules,
    crossref: &mut CrossRefMaps,
) -> Result<(), Report> {
    let tx = conn.transaction()?;
    {
        for rlink in resolved_rules.get_resolved_links() {
            let tupfile_desc = rlink.get_rule_ref().get_tupfile_desc();
            let (tupfile_db_id, _) = crossref.get_tup_db_id(&tupfile_desc).ok_or_else(|| {
                eyre!(
                    "tupfile dir not found:{:?} mentioned in rule {:?}",
                    tupfile_desc,
                    rlink.get_rule_ref()
                )
            })?;

            let mut processed_glob = HashSet::new();
            rlink.for_each_glob_path_desc(|glob_path_desc| -> Result<(), Error> {
                //               links_to_add.insert(glob_path_desc, tupfile_db_id);
                let (glob_pattern_id, _) =
                    crossref.get_glob_db_id(&glob_path_desc).ok_or_else(|| {
                        Error::new_path_search_error(format!(
                            "glob path not found:{:?} in tup db",
                            rw_buf.get_glob_path(&glob_path_desc).as_path()
                        ))
                    })?;
                if processed_glob.insert(glob_pattern_id) {
                    tx.insert_link(glob_pattern_id, tupfile_db_id, true.into(), TupF)
                        .map_err(|e| Error::new_path_search_error(e.to_string()))?;
                }
                Ok(())
            })?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub fn gather_modified_tupfiles(
    conn: &mut TupConnection,
    targets: &Vec<String>,
) -> Result<Vec<Node>> {
    let mut tupfiles = Vec::new();
    create_path_buf_temptable(conn)?;

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
    }
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
    Ok(tupfiles)
}

pub fn gather_tupfiles(conn: &mut TupConnection) -> Result<Vec<Node>> {
    let mut tupfiles = Vec::new();
    create_path_buf_temptable(conn)?;

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
    term_progress: &TermProgress,
) -> Result<()> {
    //let mut del_stmt = conn.delete_tup_rule_links_prepare()?;
    let (sender, receiver) = crossbeam::channel::unbounded();
    term_progress.set_message("Parsing Tupfiles");
    let mut crossref = CrossRefMaps::default();
    for tup_node in tupfiles.iter() {
        let tupid = parser
            .read_write_buffers()
            .add_tup_file(Path::new(tup_node.get_name()));
        crossref.add_tup_xref(tupid, tup_node.get_id(), tup_node.get_dir());
    }
    crossbeam::thread::scope(|s| -> Result<()> {
        let wg = WaitGroup::new();
        let poisoned = Arc::new(AtomicBool::new(false));
        let num_threads = std::cmp::min(MAX_THRS_DIRS, tupfiles.len());
        let mut handles = Vec::new();
        for ithread in 0..num_threads {
            let mut parser_clone = parser.clone();
            let sender = sender.clone();
            let wg = wg.clone();
            let pb = term_progress.make_progress_bar("_");
            let poisoned = poisoned.clone();
            let join_handle = s.spawn(move |_| -> Result<()> {
                for tupfile in tupfiles
                    .iter()
                    .filter(|x| !x.get_name().ends_with(".lua"))
                    .cloned()
                    .skip(ithread)
                    .step_by(num_threads)
                {
                    if poisoned.load(Ordering::SeqCst) {
                        drop(wg);
                        return Ok(());
                    }
                    let tupfile_name = tupfile.get_name();
                    debug!("Parsing :{}", tupfile_name);
                    pb.set_message(format!("Parsing :{tupfile_name}"));
                    let res = parser_clone
                        .parse_tupfile(tupfile.get_name(), sender.clone())
                        .map_err(|error| {
                            let rwbuf = parser_clone.read_write_buffers();
                            let display_str = rwbuf.display_str(&error);
                            term_progress.abandon(&pb, format!("Error parsing {tupfile_name}"));
                            poisoned.store(true, Ordering::SeqCst);
                            // drop(wg);
                            eyre!(
                                "Error while parsing tupfile: {}:\n {} due to \n{}",
                                tupfile.get_name(),
                                display_str,
                                error
                            )
                        });
                    if let Err(e) = res {
                        drop(wg);
                        return Err(e);
                    }
                    pb.set_message(format!("Done parsing {}", tupfile.get_name()));
                    term_progress.tick(&pb);
                }
                drop(wg);
                pb.set_message("Done");
                Ok(())
            });
            handles.push(join_handle);
        }
        drop(sender);

        let pb = term_progress.get_main();
        let rwbufs = parser.read_write_buffers().clone();
        //let mut crossref = CrossRefMaps::default();
        let outs = parser.get_outs();
        let parser_c = parser.clone();
        let mut insert_to_db = move |resolved_rules: ResolvedRules| -> Result<()> {
            let binding = parser_c.get_mut_searcher();
            let conn = &mut binding.conn.lock();
            let conn = conn.deref_mut();
            check_uniqueness_of_parent_rule(conn, &rwbufs, &outs, &mut crossref)?;
            insert_nodes(conn, &rwbufs, &resolved_rules, &mut crossref)?;
            let _ = insert_links(conn, &resolved_rules, &mut crossref)?;
            let (dbid, _) = crossref
                .get_tup_db_id(resolved_rules.get_tupid())
                .expect("tupfile dbid fetch failed");
            conn.delete_modified(dbid)?;
            Ok(())
        };
        let mut insert_to_db_wrap_err = move |resolved_rules: ResolvedRules| -> Result<(), Error> {
            insert_to_db(resolved_rules).map_err(|e| Error::CallBackError(e.to_string()))
        };

        pb.set_message("Resolving statements..");
        {
            for tupfile_lua in tupfiles
                .iter()
                .filter(|x| x.get_name().ends_with(".lua"))
                .cloned()
            {
                let path = tupfile_lua.get_name();
                pb.set_message(format!("Parsing :{}", path));
                let resolved_rules = parser.parse(path).map_err(|e| {
                    term_progress.abandon(&pb, format!("Error parsing {path}"));
                    eyre!("Error: {}", parser.read_write_buffers().display_str(&e))
                })?;
                //new_resolved_rules.push(resolved_rules);
                insert_to_db_wrap_err(resolved_rules)?;
                term_progress.tick(&pb);
                pb.set_message(format!("Done parsing {}", path));
            }
        }

        pb.set_message("Resolving statements..");
        parser
            .receive_resolved_statements(receiver, insert_to_db_wrap_err)
            .map_err(|error| {
                let read_write_buffers = parser.read_write_buffers();
                let tup_node = read_write_buffers.get_tup_path(error.get_tup_descriptor());
                let tup_node = tup_node.to_string();
                let display_str = read_write_buffers.display_str(error.get_error_ref());
                term_progress.abandon(&pb, format!("Error parsing {tup_node}"));
                eyre!(
                    "Unable to resolve statements in tupfile {}:\n{}",
                    tup_node.as_str(),
                    display_str,
                )
            })?;

        term_progress.finish(&pb, "Done parsing tupfiles");
        term_progress.clear();
        wg.wait();
        for join_handle in handles {
            join_handle.join().unwrap()?; // fail if any of the spawned threads returned an error
        }
        Ok(())
        //   Ok(new_resolved_rules)
    })
    .expect("Thread error while fetching resolved rules from tupfiles")?;
    let ps = parser.get_mut_searcher();
    ps.deref().update_delete_list()?;
    Ok(())
}

/// checks that in  parsed tup files, no two rules produce the same output
fn check_uniqueness_of_parent_rule(
    conn: &mut TupConnection,
    read_buf: &ReadWriteBufferObjects,
    outs: &impl OutputHandler,
    _crossref: &mut CrossRefMaps,
) -> Result<()> {
    for o in outs.get_output_files().iter() {
        let np = read_buf.get_path(o);
        if let Ok(node) = find_by_path_inner(np.as_path(), &conn.as_ref()) {
            let rule_id = node.get_srcid();

            let current_parent_rule_ref = 
                outs.get_parent_rule(o).unwrap_or_else(|| panic!("Parent rule not found for {}", o));
            if !conn.check_is_in_update_universe(rule_id)? {
                let old_rule_name = conn.fetch_node_name(rule_id)?;
                return Err(eyre!(
                            format!("File was previously marked as generated from a rule:{} \
                    but is now being generated in {}",
                                    old_rule_name, current_parent_rule_ref.to_string()
                            )
                        ));
            }
        }
    }
    Ok(())
}

fn find_by_path_inner(path: &Path, conn: &TupConnectionRef) -> Result<Node> {
   let res =  find_node_id_by_path(conn, path)
       .and_then(|node| conn.fetch_node_by_id(node).transpose().expect("Unexpected failure in retrieving node from its id"))?;
    Ok(res)
}

fn find_node_id_by_path(conn: &TupConnectionRef, path: &Path) -> Result<i64, AnyError> {
    path.components()
        .try_fold((0, PathBuf::new()), |acc, c| {
            let (dir, path_so_far) = acc;
            let name = c.as_os_str().to_string_lossy();
            let node = conn.fetch_node_id_by_dir_and_name(dir, name.as_ref())?;
            Ok((node, path_so_far.join(name.as_ref())))
        }).map(|(id, _)| id)
}

fn find_by_path(path: &Path, conn_ref: &TupConnectionRef) -> Node {
    find_by_path_inner(path, conn_ref).unwrap_or_else(|e| {
        log::warn!("Error while finding path: {:?}", e);
        Default::default()
    })
}

pub(crate) fn remove_path<P: AsRef<Path>>(conn: &TupConnection, path: P) -> Result<()> {
    // if the path is a file, add it to the deletelist table
    let connection_ref = conn.as_ref();
    let node = find_by_path(path.as_ref(), &connection_ref);
    let nodeid = node.get_id();
    if nodeid > 0 {
        //add_ids_statements.add_to_delete(nodeid, *node.get_type())?;
        conn.mark_deleted(nodeid, node.get_type())?;
        /* if node.get_type() == &Dir {
            buf_statements.remove_dirpathbuf(nodeid)?;
        } else if node.get_type() == &TupF {
            buf_statements.remove_tuppathbuf(nodeid)?;
        } */
    }
    Ok(())
}

pub(crate) fn insert_path(
    conn: &TupConnectionRef,
    path_buffers: &impl PathBuffers,
    pd: &PathDescriptor,
    cross_ref_maps: &mut CrossRefMaps,
    rtype: RowType,
) -> Result<(i64, i64)> {
    let parent_pd = path_buffers.get_parent_id(pd);
    if parent_pd.is_root() {
        return Ok((0, 0));
    }
    let rtype_parent = if rtype.is_generated() {
        DirGen // if the node parent is not inserted yet, it is a generated directory
    } else {
        Dir
    };
    let (nid, _dir, _) = parent_pd.components().try_fold(
        (0i64, -1, path_buffers.get_root_dir().to_path_buf()),
        |acc, c| -> Result<(i64, i64, PathBuf)> {
            let (dir, _, path_so_far) = acc;
            let name = c.get_file_name_os_str();
            if let Some((path_db_id, path_parent_db_id)) = cross_ref_maps.get_path_db_id(&c) {
                Ok((path_db_id, path_parent_db_id, path_so_far.join(name))) // call insert once and reuse
            } else {
                let (nid, dir) = insert_node_in_dir(
                    conn,
                    name.to_string_lossy(),
                    path_so_far.as_path(),
                    dir,
                    &rtype_parent,
                )?;
                cross_ref_maps.add_path_xref(c.clone(), nid, dir);
                Ok((nid, dir, path_so_far.join(name)))
            }
        },
    )?;
    let name = pd.get_file_name_os_str();
    insert_node_in_dir(
        conn,
        name.to_string_lossy(),
        path_buffers.get_path_ref(pd),
        nid,
        &rtype,
    )
}

fn insert_node_in_dir(
    conn: &TupConnectionRef,
    name: Cow<str>,
    path: &Path,
    dir: i64,
    rtype: &RowType,
) -> Result<(i64, i64)> {
    let pbuf = path.to_owned();
    let hashed_path = crate::scan::HashedPath::from(pbuf);
    let mut metadata = fs::metadata(path).ok();
    if metadata.as_ref().map_or(false, |m| m.is_symlink()) {
        metadata = fs::symlink_metadata(path).ok();
    }
    let is_dir = metadata.as_ref().map_or(false, |m| m.is_dir());
    if let Some(node_at_path) =
        crate::scan::prepare_node_at_path(dir, name, hashed_path.clone(), metadata, &rtype)
    {
        let in_node = node_at_path.get_prepared_node();
        let pbuf = node_at_path.get_hashed_path().clone();
        let node = conn.upsert_node(in_node, || compute_path_hash(is_dir, pbuf.clone()))?;
        Ok((node.get_id(), dir))
    } else {
        log::warn!("Error while inserting path: {:?}", path);
        Ok((-1, -1))
    }
}

pub(crate) fn compute_path_hash(is_dir: bool, pbuf: HashedPath) -> String {
    if is_dir {
        compute_dir_sha256(pbuf.as_ref()).unwrap_or_default()
    } else {
        compute_sha256(pbuf.as_ref()).unwrap_or_default()
    }
}

fn ensure_parent_inserted(
    conn: &TupConnectionRef,
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

    let (dir, pardir) = find_by_path_inner(parent_path.as_path(), conn)
        .map(|n| (n.get_id(), n.get_dir()))
        .or_else(|_| -> Result<(i64, i64)> {
            // try adding parent directory if not in db
            let (dir, pardir) =
                insert_path(conn, path_buffers, parent, cross_ref_maps, pardir_type)?;
            Ok((dir, pardir))
        })?;
    Ok((dir, pardir))
}

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

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
enum Link {
    DirToGlob(PathDescriptor, GlobPathDescriptor),
    InputToRule(PathDescriptor, RuleDescriptor),
    RuleToOutput(RuleDescriptor, PathDescriptor),
    OutputToGroup(PathDescriptor, GroupPathDescriptor),
    GroupToRule(GroupPathDescriptor, RuleDescriptor),
    EnvToRule(EnvDescriptor, RuleDescriptor),
    TupfileToTupfile(PathDescriptor, TupPathDescriptor),
    GlobToTupfile(GlobPathDescriptor, TupPathDescriptor),
    TupfileToRule(TupPathDescriptor, RuleDescriptor),
}

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
        self.links.insert(Link::DirToGlob(d.clone(), g.clone()));
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
    pub(crate) fn add_globs_read_to_tupfile_link(
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
        self.nodes_to_insert
            .push(NodeToInsert::GeneratedFile(p.clone(), srcid));
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
            .push(NodeToInsert::Glob(p.clone(), tupid));
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
            .push(NodeToInsert::InputFile(p.clone()));
        Ok(())
    }

    pub(crate) fn add_tupfile(&mut self, p0: &PathDescriptor) -> Result<()> {
        if self.processed.insert(p0.clone()) {
            self.collect_input(p0)?;
        }
        Ok(())
    }

    fn collect_rule(&mut self, p: &RuleDescriptor) {
        self.nodes_to_insert.push(NodeToInsert::Rule(p.clone()));
    }
    fn collect_task(&mut self, p: &TaskDescriptor) {
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
    fn add_task_node(&mut self, task_desc: &TaskDescriptor) -> Result<()> {
        let task_instance = self.read_write_buffer_objects.get_task(task_desc);
        let name = task_instance.get_target();
        let tuppath = task_instance.get_parent();
        let tuppathstr = tuppath.as_path();
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
            self.collect_task(task_desc);
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
    fn add_rule_node(&mut self, rule_desc: &RuleDescriptor, dir: i64) -> Result<()> {
        let rule_formula = self.read_write_buffer_objects.get_rule(rule_desc);
        let name = rule_formula.get_rule_str();
        let ref ph = self.read_write_buffer_objects;
        let tup_cwd_desc = &rule_formula.get_rule_ref().get_tup_dir();
        let tuppath = ph.get_path(&tup_cwd_desc);
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
            let prevline = self
                .unique_rule_check
                .insert(dir.to_string() + "/" + name.as_str(), rule_ref.clone());
            if prevline.is_none() {
                self.collect_rule(rule_desc);
            } else {
                bail!(
                    "Rule at  {}:{} was previously defined at {}. \
                                        Ensure that rule definitions take the inputs as arguments.",
                    tuppath.to_string().as_str(),
                    rule_ref,
                    prevline.unwrap()
                );
            }
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

// nodes to insert after rules have been resolved
fn insert_nodes(
    conn: &mut TupConnection,
    read_write_buf: &ReadWriteBufferObjects,
    resolved_rules: &ResolvedRules,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    //let rules_in_tup_file = resolved_rules.rules_by_tup();

    //let mut nodeids = BTreeSet::new();
    //let mut paths_to_update: HashMap<i64, i64> = HashMap::new();  we dont update nodes until rules are executed.
    let mut envs_to_insert = HashSet::new();

    let get_dir = |tup_desc: &TupPathDescriptor, crossref: &CrossRefMaps| -> Result<i64> {
        crossref.get_tup_db_id(tup_desc)
            .map(|(_, dir)| dir)
            .or_else(|| crossref.get_path_db_id(&tup_desc.get_parent_descriptor()).map(|(dir, _)| dir))
            .ok_or_else(|| eyre!("No tup directory found in db for tup descriptor:{:?}", tup_desc))
    };
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
                collector.add_task_node(rd)?;
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
            let dir = get_dir(&tup_desc, &crossref)?;
            collector.add_rule_node(rd, dir)?;
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
        }
        collector.nodes()
    };
    // sort nodes by type and then by path for better performance (as parent directories are inserted first)
    nodes.sort_by(|a, b| {
        if a.get_type().eq(&b.get_type()) {
            a.get_path(&read_write_buf)
                .cmp(&b.get_path(&read_write_buf))
        } else {
            a.get_type().cmp(&b.get_type())
        }
    });

    let tx = conn.transaction()?;
    {
        let parent_descriptors = nodes
            .iter()
            .map(|n| (n.get_parent_id(read_write_buf), n.get_type()));

        for (parent_desc, rowtype) in parent_descriptors {
            if !parent_desc.is_root() && crossref.get_path_db_id(&parent_desc).is_none() {
                let connection_ref = tx.connection();
                let (parid, parparid) = ensure_parent_inserted(
                    &connection_ref,
                    read_write_buf.get(),
                    crossref,
                    &rowtype,
                    &parent_desc,
                )?;
                if !parid.is_negative() {
                    crossref.add_path_xref(parent_desc, parid, parparid);
                }
            }
        }
        for node_to_insert in &nodes {
            debug!("inserting node: {:?}", node_to_insert);
            let node = node_to_insert.get_node(&read_write_buf, crossref)?;
            let compute_sha = || {
                let tup_connection_ref = tx.connection();
                node_to_insert
                    .compute_node_sha(&tup_connection_ref, read_write_buf.get())
                    .unwrap_or_default()
            };
            let (db_id, db_par_id) = {
                let upsnode = tx.upsert_node(&node, compute_sha)?;
                (upsnode.get_id(), upsnode.get_dir())
            };
            if !db_id.is_negative() {
                node_to_insert.update_crossref(crossref, db_id, db_par_id);
                let sha = compute_sha();
                if !sha.is_empty() {
                    tx.upsert_node_sha(db_id, &sha)?;
                }
            } else {
                log::warn!("Failed to insert node: {:?}", node);
            }
        }

        for env_var in envs_to_insert.iter() {
            let env_val = env_var.get().get_val_str();
            let key = env_var.get().get_key_str();
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
        }
    }
    tx.commit()?;

    // some validity checks after inserting nodes to see if the db is consistent
    let rules = resolved_rules.rules_by_tup();
    let tasks = resolved_rules.tasks_by_tup();
    let srcs_from_links = {
        log::info!(
            "Cross referencing rule inputs to insert with the db ids with same name and directory"
        );
        rules
            .iter()
            .flat_map(|rl| rl.iter())
            .flat_map(|rl| rl.get_sources().map(|s| (rl.get_rule_ref(), s)))
    };
    let srcs_from_tasks = {
        log::info!(
            "Cross referencing task inputs to insert with the db ids with same name and directory"
        );
        tasks
            .iter()
            .flat_map(|rl| rl.iter())
            .flat_map(|rl| rl.get_deps().iter().map(|s| (rl.get_task_loc(), s)))
    };
    for (i, s) in srcs_from_links.chain(srcs_from_tasks) {
        if let Some(pd) = s.get_resolved_path_desc() {
            let (_, _) = crossref
                .get_path_db_id(&pd.get_parent_descriptor())
                .ok_or(eyre!(
                    "parent path not found:{:?} mentioned in rule {:?}",
                    pd.get_parent_descriptor(),
                    i
                ))?;
            let (_, _) = crossref
                .get_path_db_id(pd)
                .ok_or_else(|| eyre!("path not found:{:?} mentioned in rule {:?}", s, i))?;
        }
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

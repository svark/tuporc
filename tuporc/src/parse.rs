use bimap::BiMap;
use crossbeam::sync::WaitGroup;
use eyre::{bail, eyre, OptionExt, Report, Result};
use log::debug;
use parking_lot::lock_api::MutexGuard;
use parking_lot::{Mutex, RawMutex};
use rusqlite::Connection;
use std::borrow::Cow;
// use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fs;
use std::ops::Deref;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tupparser::buffers::{
    EnvDescriptor, GlobPathDescriptor, GroupPathDescriptor, OutputHolder, PathBuffers,
    PathDescriptor, RuleDescriptor, TaskDescriptor, TupPathDescriptor,
};
use tupparser::decode::{OutputHandler, PathSearcher};
use tupparser::errors::Error;
use tupparser::paths::{GlobPath, InputResolvedType, MatchingPath, NormalPath};
use tupparser::{ReadWriteBufferObjects, ResolvedRules, TupParser};

use crate::db::RowType::{Dir, DirGen, Env, Excluded, File, GenF, Glob, Rule, TupF};
use crate::db::{
    create_path_buf_temptable, db_path_str, AnyError, CallBackError, ForEachClauses, LibSqlExec,
    MiscStatements, SqlStatement,
};
use crate::scan::MAX_THRS_DIRS;
use crate::{LibSqlPrepare, Node, RowType, TermProgress};

// CrossRefMaps maps paths, groups and rules discovered during parsing with those found in database
// These are two ways maps, so you can query both ways
#[derive(Debug, Clone, Default)]
pub struct CrossRefMaps {
    group_id: BiMap<GroupPathDescriptor, (i64, i64)>,
    // group id and the corresponding db id,, parent id
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

type SrcId = RuleDescriptor;
#[derive(Debug, Clone)]
enum NodeToInsert {
    Rule(RuleDescriptor),
    #[allow(dead_code)]
    Tup(TupPathDescriptor),
    Group(GroupPathDescriptor),
    GeneratedFile(PathDescriptor, SrcId),
    Env(EnvDescriptor),
    Task(TaskDescriptor),
    #[allow(dead_code)]
    InputFile(PathDescriptor),
    Glob(GlobPathDescriptor),
    ExcludedFile(PathDescriptor),
}

impl NodeToInsert {
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
            NodeToInsert::Glob(g) => read_write_buffer_objects
                .get_glob_path(g)
                .file_name()
                .to_string(),
            NodeToInsert::ExcludedFile(e) => read_write_buffer_objects.get_name(e),
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
            NodeToInsert::Glob(g) => g.to_u64() as i64,
            NodeToInsert::ExcludedFile(p) => p.to_u64() as i64,
        }
    }

    pub fn get_row_type(&self) -> RowType {
        match self {
            NodeToInsert::Rule(_) => Rule,
            NodeToInsert::Tup(_) => RowType::TupF,
            NodeToInsert::Group(_) => RowType::Grp,
            NodeToInsert::GeneratedFile(_, _) => GenF,
            NodeToInsert::Env(_) => Env,
            NodeToInsert::Task(_) => RowType::Task,
            NodeToInsert::InputFile(_) => File,
            NodeToInsert::Glob(_) => Glob,
            NodeToInsert::ExcludedFile(_) => Excluded,
        }
    }

    pub fn get_srcid(&self, cross_ref_maps: &CrossRefMaps) -> Result<i64> {
        match self {
            NodeToInsert::GeneratedFile(gen, id) => cross_ref_maps
                .get_rule_db_id(id)
                .map(|x| x.0)
                .ok_or_eyre(eyre!(
                    "srcid not found for generated file: {:?} with rule descriptor:{:?}",
                    gen,
                    id
                )),
            _ => Ok(-1),
        }
    }
    pub fn get_display_str(&self, read_write_buffer_objects: &ReadWriteBufferObjects) -> String {
        match self {
            NodeToInsert::Rule(r) => read_write_buffer_objects.get_rule(r).get_display_str(),
            NodeToInsert::Env(e) => read_write_buffer_objects.get_env_value(e),
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
            NodeToInsert::Glob(g) => read_write_buffer_objects.get_glob_parent_id(g),
            NodeToInsert::ExcludedFile(e) => read_write_buffer_objects.get_parent_id(e),
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
            RowType::DirGen | RowType::Dir | RowType::Excluded | RowType::Glob => Node::new(
                self.get_id(),
                parent_id,
                0,
                self.get_name(read_write_buffer_objects),
                self.get_row_type(),
            ),
            RowType::Rule => Node::new_rule(
                self.get_id(),
                parent_id,
                self.get_name(read_write_buffer_objects),
                self.get_display_str(read_write_buffer_objects),
                self.get_flags(read_write_buffer_objects),
                0,
            ),
            RowType::TupF | RowType::File | RowType::GenF => Node::new_file_or_genf(
                self.get_id(),
                parent_id,
                0,
                self.get_name(read_write_buffer_objects),
                self.get_row_type(),
                self.get_srcid(crossref)?, // at this point we know that rules have been inserted it is generated
            ),
            RowType::Env => Node::new_env(
                self.get_id(),
                parent_id,
                self.get_name(read_write_buffer_objects),
                self.get_display_str(read_write_buffer_objects),
            ),
            RowType::Grp => Node::new_grp(
                self.get_id(),
                parent_id,
                self.get_name(read_write_buffer_objects),
            ),
            RowType::Task => Node::new_task(
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
            NodeToInsert::Glob(g) => {
                crossref.add_path_xref(g.clone(), id, parid);
            }
            NodeToInsert::ExcludedFile(p) => {
                crossref.add_path_xref(p.clone(), id, parid);
            }
        }
    }
}
impl CrossRefMaps {
    pub fn get_group_db_id(&self, g: &GroupPathDescriptor) -> Option<(i64, i64)> {
        self.group_id.get_by_left(g).copied()
    }
    pub fn get_path_db_id(&self, p: &PathDescriptor) -> Option<(i64, i64)> {
        if p.eq(&PathDescriptor::default()) {
            Some((1, 0))
        } else {
            self.path_id.get_by_left(p).copied()
        }
    }
    pub fn get_rule_db_id(&self, r: &RuleDescriptor) -> Option<(i64, i64)> {
        self.rule_id.get_by_left(r).copied()
    }

    pub fn get_tup_db_id(&self, r: &TupPathDescriptor) -> Option<(i64, i64)> {
        self.tup_id.get_by_left(r).copied()
    }

    pub fn get_env_db_id(&self, e: &EnvDescriptor) -> Option<i64> {
        self.env_id.get_by_left(e).copied()
    }

    pub fn get_glob_db_id(&self, s: &GlobPathDescriptor) -> Option<(i64, i64)> {
        self.path_id.get_by_left(&s).copied()
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

/// Path searcher that scans the sqlite database for matching paths
/// It is used by the parser to resolve paths and globs. Resolved outputs are dumped in OutputHolder
#[derive(Debug, Clone)]
struct DbPathSearcher {
    conn: Arc<Mutex<Connection>>,
    psx: OutputHolder,
    root: std::path::PathBuf,
    last_search_dir: Arc<Mutex<PathDescriptor>>,
}

impl DbPathSearcher {
    pub fn new<P: AsRef<Path>>(conn: Connection, root: P) -> DbPathSearcher {
        DbPathSearcher {
            conn: Arc::new(Mutex::new(conn)),
            psx: OutputHolder::new(),
            root: root.as_ref().to_path_buf(),
            last_search_dir: Arc::new(Mutex::new(PathDescriptor::default())),
        }
    }

    pub fn get_last_search_dir(&self) -> MutexGuard<'_, RawMutex, PathDescriptor> {
        self.last_search_dir.lock()
    }
    pub fn set_last_search_dir(&self, pd: PathDescriptor) {
        if let Some(mut v) = self.last_search_dir.try_lock() {
            if *v != pd {
                *v = pd;
            }
        }
    }
    fn fetch_glob_nodes(
        &self,
        ph: &impl PathBuffers,
        glob_paths: &[GlobPath], // glob paths to search for (corresponding to search dirs that were specified before)
    ) -> std::result::Result<Vec<MatchingPath>, AnyError> {
        let mut index_to_try_first = 0;
        {
            let last_search_dir = self.get_last_search_dir();
            if let Some((i, _)) = glob_paths
                .iter()
                .enumerate()
                .find(|(_, g)| g.get_base_desc().eq(last_search_dir.deref()))
            {
                index_to_try_first = i;
            }
        }

        if index_to_try_first != 0 {
            let (first, rest) = glob_paths.split_at(index_to_try_first);
            let mut new_glob_paths = Vec::with_capacity(glob_paths.len());
            new_glob_paths.extend_from_slice(&rest);
            new_glob_paths.extend_from_slice(&first);
            return self.fetch_glob_nodes(ph, &new_glob_paths);
        }
        let conn = self.conn.deref().lock();
        let recursive = glob_paths[0].is_recursive_prefix();

        let mut glob_query = conn.fetch_glob_nodes_prepare(recursive)?;
        //debug!("base path is : {:?}", ph.get_path(base_path));

        for glob_path in glob_paths {
            let has_glob_pattern = glob_path.has_glob_pattern();
            if has_glob_pattern {
                debug!(
                    "looking for matches in db for glob pattern: {:?}",
                    glob_path.get_abs_path()
                );
            }

            let base_path = glob_path.get_base_desc();
            debug!("base path:{:?}", base_path);
            let tup_cwd = glob_path.get_tup_dir_desc();
            //debug!("base path is : {:?}", ph.get_path(base_path));
            let glob_pattern = ph.get_rel_path(&glob_path.get_glob_path_desc(), base_path);
            debug!("glob pattern is : {:?}", glob_pattern);
            if glob_pattern.to_string().contains("$(") {
                log::error!("unexpected path to search!");
            }
            let fetch_row = |s: &String| -> Option<MatchingPath> {
                debug!("found:{} at {:?}", s, base_path);
                let full_path_pd = base_path.join(s.as_str()).ok()?;
                if has_glob_pattern {
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
                }
            };

            let mps = glob_query.fetch_glob_nodes(
                base_path.get_path_ref().as_path(),
                glob_pattern.as_path(),
                recursive,
                fetch_row,
            );
            match mps {
                Ok(mps) => {
                    if !mps.is_empty() {
                        self.set_last_search_dir(base_path.clone());
                        return Ok(mps);
                    }
                }
                Err(e) if e.has_no_rows() => {
                    debug!("no rows found for glob pattern: {:?}", glob_pattern);
                }
                Err(e) => return Err(e),
            }
        }
        log::error!("no rows found for any glob pattern: {:?}", glob_paths);
        Err(AnyError::Db(rusqlite::Error::QueryReturnedNoRows))
    }
}

impl PathSearcher for DbPathSearcher {
    fn discover_paths(
        &self,
        path_buffers: &impl PathBuffers,
        glob_path: &[GlobPath],
    ) -> Result<Vec<MatchingPath>, Error> {
        let mps = self.fetch_glob_nodes(path_buffers, glob_path);
        let mut mps = match mps {
            Ok(mps) => Ok(mps),
            Err(e) if e.has_no_rows() => self
                .get_outs()
                .discover_paths(path_buffers, glob_path)
                .map_err(|e| Error::new_path_search_error(e.to_string())),
            Err(e) => Err(Error::new_path_search_error(e.to_string())),
        }?;
        let c = |x: &MatchingPath, y: &MatchingPath| {
            let x = x.path_descriptor();
            let y = y.path_descriptor();
            x.cmp(&y)
        };
        mps.sort_by(c);
        mps.dedup();
        Ok(mps)
    }

    fn locate_tuprules(
        &self,
        tup_cwd: &PathDescriptor,
        path_buffers: &impl PathBuffers,
    ) -> Vec<PathDescriptor> {
        let conn = self.conn.lock();
        let mut node_path_stmt = conn.deref().fetch_node_path_prepare().unwrap();
        let tup_path = tup_cwd.get_path_ref();
        debug!(
            "tup path is : {} in which (or its parents) we look for TupRules.tup or Tuprules.lua",
            tup_path.as_path().display()
        );
        let dirid = conn
            .deref()
            .fetch_dirid_prepare()
            .and_then(|mut stmt| stmt.fetch_dirid(tup_path.as_path()))
            .unwrap_or(1i64);
        debug!("dirid: {}", dirid);
        let mut tup_rules = Vec::new();
        let tuprs = ["tuprules.tup", "tuprules.lua"];
        let mut add_rules = |dirid| {
            for tupr in tuprs {
                if let Ok((name, dirid_)) = conn
                    .first_containing(tupr, dirid)
                    .inspect_err(|e| eprintln!("Error while looking for tuprules: {}", e))
                {
                    debug!("tup rules node  : {} dir:{}", name, dirid_);
                    let node_dir = node_path_stmt.fetch_node_dir_path(dirid_).unwrap();
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
    connection: Connection,
    tupfiles: Vec<Node>,
    root: P,
    term_progress: &TermProgress,
    var: &String,
) -> Result<Vec<(TupPathDescriptor, String)>> {
    {
        let conn = connection;

        let db = DbPathSearcher::new(conn, root.as_ref());
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
                    states_after_parse.push((statements_to_resolve.get_tup_desc().clone(), val));
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
    connection: Connection,
    tupfiles: Vec<Node>,
    root: P,
    term_progress: &TermProgress,
) -> Result<()> {
    let mut crossref = CrossRefMaps::default();
    let (resolved_rules, mut rwbufs, mut outs) = {
        let conn = connection;

        let db = DbPathSearcher::new(conn, root.as_ref());
        let mut parser = TupParser::try_new_from(root.as_ref(), db)?;
        let mut visited = BTreeSet::new();
        {
            let dbref = parser.get_mut_searcher();
            let mut conn = dbref.conn.deref().lock();
            let tx = conn.transaction()?;
            {
                let mut del_normal_link = tx.delete_tup_rule_links_prepare()?;
                let mut s = tx.fetch_rules_nodes_prepare_by_dirid()?;
                let mut del_outputs = tx.mark_outputs_deleted_prepare()?;
                //let mut fetch_rule_deps = tx.get_rule_deps_tupfiles_prepare()?;
                let mut tupfile_to_process = VecDeque::from(tupfiles);
                while let Some(tupfile) = tupfile_to_process.pop_front() {
                    let dir = tupfile.get_dir();
                    if visited.insert(tupfile) {
                        let rules = s.fetch_rule_nodes_by_dirid(dir)?;
                        for r in rules {
                            del_normal_link.delete_normal_rule_links(r.get_id())?;
                            del_outputs.mark_rule_outputs_deleted(r.get_id())?;
                        }
                    }
                    //pb.inc(1);
                }
            }
            tx.commit()?;
        }
        let tupfiles: Vec<_> = visited.into_iter().collect();
        let resolved_rules =
            gather_rules_from_tupfiles(&mut parser, tupfiles.as_slice(), &term_progress)?;
        let resolved_rules = parser.reresolve(resolved_rules)?;
        for tup_node in tupfiles.iter() {
            let tupid = parser
                .read_write_buffers()
                .add_tup_file(Path::new(tup_node.get_name()));
            crossref.add_tup_xref(tupid, tup_node.get_id(), tup_node.get_dir());
        }
        (
            resolved_rules,
            parser.read_write_buffers(),
            parser.get_outs().clone(),
        )
    };
    let mut conn = Connection::open(".tup/db")
        .expect("Connection to tup database in .tup/db could not be established");

    let _ = insert_nodes(&mut conn, &rwbufs, &resolved_rules, &mut crossref)?;

    check_uniqueness_of_parent_rule(&mut conn, &rwbufs, &outs, &mut crossref)?;

    add_group_providers(&mut conn, &resolved_rules, &crossref)?;
    fetch_group_providers(&mut conn, &mut rwbufs, &mut outs, &mut crossref)?;
    add_rule_links(&mut conn, &rwbufs, &resolved_rules, &mut crossref)?;
    // add links from glob inputs to tupfiles's directory
    add_link_glob_dir_to_rules(&mut conn, &rwbufs, &resolved_rules, &mut crossref)?;
    Ok(())
}

/// adds links from glob patterns specified at each directory that are inputs to rules  to the tupfile directory
/// We don't directly add links from glob patterns to rules but instead add links from glob patterns to the tupfile so that they are parsed whenever glob patterns are modified
/// Newer / modified /deleted inputs discovered in glob patterns and added as rule inputs will be process in a re-iterations parsing phase of Tupfile which the glob pattern links to
fn add_link_glob_dir_to_rules(
    conn: &mut Connection,
    rw_buf: &ReadWriteBufferObjects,
    resolved_rules: &ResolvedRules,
    crossref: &mut CrossRefMaps,
) -> Result<(), Report> {
    let tx = conn.transaction()?;
    {
        let mut insert_link = tx.insert_link_prepare()?;
        for rlink in resolved_rules.get_resolved_links() {
            let tupfile_desc = rlink.get_tup_loc().get_tupfile_desc();
            let (tupfile_db_id, _) = crossref.get_tup_db_id(tupfile_desc).ok_or_else(|| {
                eyre!(
                    "tupfile dir not found:{:?} mentioned in rule {:?}",
                    tupfile_desc,
                    rlink.get_tup_loc()
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
                    insert_link
                        .insert_link(glob_pattern_id, tupfile_db_id, true, RowType::TupF)
                        .map_err(|e| Error::new_path_search_error(e.to_string()))?;
                }
                Ok(())
            })?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub fn gather_modified_tupfiles(conn: &mut Connection, targets: &Vec<String>) -> Result<Vec<Node>> {
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
    conn.for_changed_or_created_tup_node_with_path(|n: Node| {
        // name stores full path here
        log::debug!("tupfile to parse:{}", n.get_name());
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

pub fn gather_tupfiles(conn: &mut Connection) -> Result<Vec<Node>> {
    let mut tupfiles = Vec::new();
    create_path_buf_temptable(conn)?;

    conn.for_tup_node_with_path(|n: Node| {
        // the field `name` in n stores full path here
        tupfiles.push(n);
        Ok(())
    })?;
    Ok(tupfiles)
}

fn gather_rules_from_tupfiles(
    p: &mut TupParser<DbPathSearcher>,
    tupfiles: &[Node],
    term_progress: &TermProgress,
) -> Result<ResolvedRules> {
    //let mut del_stmt = conn.delete_tup_rule_links_prepare()?;
    let mut new_resolved_rules = ResolvedRules::new();
    let (sender, receiver) = crossbeam::channel::unbounded();
    term_progress.set_message("Parsing Tupfiles");
    crossbeam::thread::scope(|s| -> Result<ResolvedRules> {
        let wg = WaitGroup::new();
        let poisoned = Arc::new(AtomicBool::new(false));
        let num_threads = std::cmp::min(MAX_THRS_DIRS, tupfiles.len());
        let mut handles = Vec::new();
        for ithread in 0..num_threads {
            let mut p = p.clone();
            let sender = sender.clone();
            let wg = wg.clone();
            let pb = term_progress.make_progress_bar("_");
            let poisoned = poisoned.clone();
            let join_handle = s.spawn(move |_| -> Result<()> {
                for tupfile in tupfiles
                    .iter()
                    .filter(|x| !x.get_name().ends_with(".lua"))
                    .cloned()
                    .skip(ithread as usize)
                    .step_by(num_threads as usize)
                {
                    if poisoned.load(Ordering::SeqCst) {
                        drop(wg);
                        return Ok(());
                    }
                    let tupfile_name = tupfile.get_name();
                    log::debug!("Parsing :{}", tupfile_name);
                    pb.set_message(format!("Parsing :{tupfile_name}"));
                    let res =
                        p.parse_tupfile(tupfile.get_name(), sender.clone())
                            .map_err(|error| {
                                let rwbuf = p.read_write_buffers();
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
        pb.set_message("Resolving statements..");
        for tupfile_lua in tupfiles
            .iter()
            .filter(|x| x.get_name().ends_with(".lua"))
            .cloned()
        {
            let path = tupfile_lua.get_name();
            pb.set_message(format!("Parsing :{}", path));
            let resolved_rules = p.parse(path).map_err(|ref e| {
                term_progress.abandon(&pb, format!("Error parsing {path}"));
                eyre!("Error: {}", p.read_write_buffers().display_str(e))
            })?;
            new_resolved_rules.extend(resolved_rules);
            term_progress.tick(&pb);
            pb.set_message(format!("Done parsing {}", path));
        }
        pb.set_message("Resolving statements..");
        let resolved_rules = p.receive_resolved_statements(receiver).map_err(|error| {
            let read_write_buffers = p.read_write_buffers();
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
        new_resolved_rules.extend(resolved_rules);
        term_progress.finish(&pb, "Done parsing tupfiles");
        term_progress.clear();
        wg.wait();
        for join_handle in handles {
            join_handle.join().unwrap()?; // fail if any of the spawned threads returned an error
        }
        Ok(new_resolved_rules)
    })
    .expect("Thread error while fetching resolved rules from tupfiles")
}

/// checks that in  parsed tup files, no two rules produce the same output
fn check_uniqueness_of_parent_rule(
    conn: &mut Connection,
    read_buf: &ReadWriteBufferObjects,
    outs: &impl OutputHandler,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let mut parent_rule = conn.fetch_parent_rule_prepare()?;
    let mut fetch_rule = conn.fetch_node_by_id_prepare()?;
    for o in outs.get_output_files().iter() {
        let (db_id_of_o, _) = crossref.get_path_db_id(o).unwrap_or_else(|| {
            panic!(
                "output which was which was expected to be db is not {:?}",
                read_buf.get_path(o)
            )
        });
        if let Ok(rule_id) = parent_rule.fetch_parent_rule(db_id_of_o) {
            if rule_id.len() > 1 {
                let node = fetch_rule.fetch_node_by_id(*rule_id.first().unwrap())?;
                let parent_rule_ref = outs.get_parent_rule(o).unwrap_or_else(|| {
                    panic!(
                        "unable to fetch parent rule for output {:?}",
                        read_buf.get_path(o)
                    )
                });
                let tup_path = read_buf.get_tup_path(parent_rule_ref.get_tupfile_desc());
                //let rule_str = parent_rule_ref.to_string();
                {
                    return Err(eyre!(
                        format!("File was previously marked as generated from a rule:{} but is now being generated in Tupfile {} line:{}",
                                node.get_name(), tup_path.to_string(), parent_rule_ref.get_line()
                        )
                    ));
                }
            }
        }
    }
    Ok(())
}

/// add links to/from rules to their inputs and outputs
fn add_rule_links(
    conn: &mut Connection,
    rbuf: &ReadWriteBufferObjects,
    resolved_rules: &ResolvedRules,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let rules_in_tup_file = resolved_rules.rules_by_tup();
    let tconn = conn.transaction()?;
    {
        let mut inp_linker = tconn.insert_link_prepare()?;
        let mut out_linker = tconn.insert_link_prepare()?;
        for r in rules_in_tup_file {
            for rl in r {
                let (rule_node_id, _) = crossref
                    .get_rule_db_id(rl.get_rule_desc())
                    .expect("rule dbid fetch failed");
                let mut processed = HashSet::new();
                let mut processed_group = HashSet::new();
                let env_desc = rl.get_env_list();
                log::info!(
                    "adding links from envs  {:?} to rule: {:?}",
                    env_desc,
                    rule_node_id
                );
                for env_var in env_desc.iter() {
                    let env_id = crossref.get_env_db_id(&env_var).ok_or_else(|| {
                        eyre!("database env id not found for env var: {}", env_var)
                    })?;
                    inp_linker.insert_link(env_id, rule_node_id, false, Rule)?;
                }
                log::info!(
                    "adding links from inputs  {:?} to rule: {:?}",
                    rl.get_sources(),
                    rule_node_id
                );
                let tup_files_read = rl.get_tupfiles_read();
                for tupfile in tup_files_read {
                    let (tup_id, _) = crossref
                        .get_tup_db_id(&tupfile)
                        .or_else(|| crossref.get_path_db_id(tupfile))
                        .ok_or_else(|| eyre!("tupfile not found in db:{:?}", tupfile))?;
                    inp_linker.insert_link(tup_id, rule_node_id, false, Rule)?;
                }
                for i in rl.get_sources() {
                    match i {
                        InputResolvedType::UnResolvedGroupEntry(g) => {
                            let (group_id, _) = crossref.get_group_db_id(&g).ok_or_else(|| {
                                eyre!("db group not found with descriptor {:?}", g)
                            })?;
                            inp_linker.insert_link(group_id, rule_node_id, true, Rule)?;
                        }
                        InputResolvedType::Deglob(mp) => {
                            let (pid, _) = crossref
                                .get_path_db_id(mp.path_descriptor_ref())
                                .ok_or_else(|| {
                                    eyre!(
                                        "db path not found with descriptor {:?} => {:?}",
                                        mp.path_descriptor(),
                                        rbuf.get_input_path_str(&i)
                                    )
                                })?;
                            debug!("slink {} => {}", pid, rule_node_id);
                            if processed.insert(pid) {
                                inp_linker.insert_link(pid, rule_node_id, true, Rule)?;
                            }
                        }
                        InputResolvedType::BinEntry(_, p) => {
                            let (pid, _) = crossref
                                .get_path_db_id(&p)
                                .ok_or_else(|| eyre!("bin entry not found in db:{:?}", p))?;
                            debug!("bin slink {} => {}", pid, rule_node_id);
                            if processed.insert(pid) {
                                inp_linker.insert_link(pid, rule_node_id, true, Rule)?;
                            }
                        }
                        InputResolvedType::GroupEntry(g, p) => {
                            let (group_id, _) = crossref.get_group_db_id(&g).ok_or_else(|| {
                                eyre!("db group not found with descriptor {:?}", g)
                            })?;
                            debug!("group link {} => {}", group_id, rule_node_id);
                            if processed_group.insert(group_id) {
                                inp_linker.insert_link(group_id, rule_node_id, true, Rule)?;
                            }
                            if let Some((pid, _)) = crossref.get_path_db_id(&p) {
                                if processed.insert(pid) {
                                    inp_linker.insert_link(pid, rule_node_id, true, Rule)?;
                                }
                            }
                        }
                        InputResolvedType::UnResolvedFile(_) => {
                            let fname = rbuf.get_input_path_str(&i);
                            bail!(
                                "could not add a link from input {} to ruleid:{}",
                                fname,
                                rule_node_id
                            );
                        }
                        InputResolvedType::TaskRef(_) => {
                            bail!("Task reference cannot be an input to a rule:{}. Tasks can accept rules as input but not vice versa", rule_node_id);
                        }
                    }
                }
                {
                    log::info!(
                        "adding links from rule  {:?} to outputs: {:?}",
                        rule_node_id,
                        rl.get_targets()
                    );

                    if let Some(g) = rl.get_group_desc() {
                        debug!("adding links from rule  {:?} to grp: {:?}", rule_node_id, g);
                        let (g, _) = crossref
                            .get_group_db_id(g)
                            .ok_or_else(|| eyre!("failed to fetch db id of group {:?}", g))?;
                        out_linker.insert_link(rule_node_id, g, true, RowType::Grp)?;
                        for t in rl.get_targets() {
                            let (p, _) = crossref
                                .get_path_db_id(t)
                                .ok_or_else(|| eyre!("failed to fetch db id of path {}", t))?;
                            out_linker.insert_link(p, g, true, RowType::Grp)?;
                            out_linker.insert_link(rule_node_id, p, true, GenF)?;
                        }
                    } else {
                        for t in rl.get_targets() {
                            let (p, _) = crossref
                                .get_path_db_id(t)
                                .ok_or_else(|| eyre!("failed to fetch db id of path {}", t))?;
                            out_linker.insert_link(rule_node_id, p, true, GenF)?;
                        }
                    }
                    for t in rl.get_excluded_targets() {
                        let (p, _) = crossref
                            .get_path_db_id(t)
                            .ok_or_else(|| eyre!("failed to fetch db id of path {}", t))?;
                        out_linker.insert_link(rule_node_id, p, true, Excluded)?;
                    }
                }
            }
        }
    }
    tconn.commit()?;
    Ok(())
}

/// get a global list of files corresponding  to each group
fn fetch_group_providers(
    conn: &mut Connection,
    rwbuf: &mut ReadWriteBufferObjects,
    outs: &mut OutputHolder,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let mut group_ids = Vec::new();
    let fetch_db_ids = |group_desc: &GroupPathDescriptor| {
        let group_id = crossref
            .get_group_db_id(group_desc)
            .ok_or(Error::new_path_search_error(format!(
                "could not fetch groupid from its internal id:{:?}",
                group_desc.get()
            )))?
            .0;
        group_ids.push((group_desc.clone(), group_id));
        Ok(())
    };
    rwbuf.for_each_group(fetch_db_ids)?;

    for (group_desc, groupid) in group_ids {
        conn.for_each_grp_node_provider(
            groupid,
            Some(GenF),
            |node: Node| -> std::result::Result<(), AnyError> {
                // name of node is actually its path
                // merge providers of this group from all available in db
                let pd = rwbuf.add_abs(Path::new(node.get_name())).map_err(|_| AnyError::CbErr(
                    CallBackError::from(format!(
                        "Failed to add path {} to path buffers. Make sure it is relative to the root directory",
                        node.get_name()
                    )),
                ))?;
                outs.add_group_entry(&group_desc, pd);
                Ok(())
            },
        )?;
    }

    Ok(())
}

fn find_by_path_inner<'a>(
    path: &'a Path,
    node_statements: &mut NodeStatements,
    rtype: &RowType,
) -> Result<(Node, i64)> {
    let (parent, name) = split_path(path);
    let (dir, parent_id) = node_statements.fetch_dirid_with_par(parent.as_ref())?;
    let i = node_statements
        .fetch_node(name.as_ref(), dir)
        .unwrap_or_else(|e| {
            log::warn!("Error while finding path: {:?}", e);
            Node::unknown_with_dir(dir, name.as_ref(), rtype)
        });
    Ok((i, parent_id))
}
fn find_by_path<'a>(
    path: &'a Path,
    node_statements: &mut NodeStatements,
    rtype: &RowType,
) -> (Node, i64) {
    find_by_path_inner(path, node_statements, rtype).unwrap_or_else(|e| {
        log::warn!("Error while finding path: {:?}", e);
        (Default::default(), -1)
    })
}

pub(crate) fn remove_path<P: AsRef<Path>>(
    path: P,
    node_statements: &mut NodeStatements,
    add_ids_statements: &mut AddIdsStatements,
) -> Result<()> {
    // if the path is a file, add it to the deletelist table
    let (node, _) = find_by_path(path.as_ref(), node_statements, &RowType::File);
    let nodeid = node.get_id();
    if nodeid > 0 {
        add_ids_statements.add_to_delete(nodeid, *node.get_type())?;
        if node.get_type() == &Dir {
            node_statements.remove_dirpathbuf(nodeid)?;
        } else if node.get_type() == &TupF {
            node_statements.remove_tuppathbuf(nodeid)?;
        }
    }
    Ok(())
}

pub(crate) fn insert_path<P: AsRef<Path>>(
    path: P,
    node_statements: &mut NodeStatements,
    add_ids_statements: &mut AddIdsStatements,
    recurse: bool,
) -> Result<(i64, i64)> {
    let (parent, name) = split_path(path.as_ref());
    if parent.as_os_str().is_empty() || parent.as_os_str().eq(".") {
        return Ok((0, 0));
    }
    let dir = node_statements
        .fetch_dirid_with_par(parent.as_ref())
        .map(|(dir, _)| dir)
        .or_else(|_| -> Result<i64> {
            // try adding parent directory if not in db
            let (dir, _) = insert_path(
                parent.as_ref(),
                node_statements,
                add_ids_statements,
                recurse,
            )?;
            Ok(dir)
        })?;

    if dir.is_negative() {
        return Ok((dir, -1));
    }

    let mut insert_in_dir = |name: Cow<str>, path: &Path, dir: i64| -> Result<(i64, i64)> {
        let pbuf = path.to_owned();
        let hashed_path = crate::scan::HashedPath::from(pbuf);
        let metadata = fs::metadata(path).ok();
        if let Some(node_at_path) =
            crate::scan::prepare_node_at_path(dir, name, hashed_path, metadata)
        {
            let in_node = node_at_path.get_prepared_node();
            let node = find_upsert_node(node_statements, add_ids_statements, in_node)?;
            // add to auxiliary tables : dirpathbuf if it is a directory and Tuppathbuf if it is a tupfile
            if node.get_type() == &Dir {
                node_statements
                    .add_to_dirpathbuf(node.get_id(), node_at_path.get_hashed_path().as_ref())?;
            }
            if node.get_type() == &TupF {
                node_statements
                    .add_to_tuppathbuf(node.get_id(), node_at_path.get_hashed_path().as_ref())?;
            }
            Ok((node.get_id(), dir))
        } else {
            log::warn!("Error while inserting path: {:?}", path);
            Ok((-1, -1))
        }
    };
    insert_in_dir(name, path.as_ref(), dir)
}

fn split_path(path: &Path) -> (Cow<Path>, Cow<'_, str>) {
    let parent = tupparser::transform::get_parent(path);
    let name = path.file_name().unwrap_or("".as_ref()).to_string_lossy();
    (parent, name)
}
struct Collector {
    processed_globs: HashSet<PathDescriptor>,
    processed: HashSet<PathDescriptor>,
    processed_outputs: HashSet<PathDescriptor>,
    nodes_to_insert: Vec<NodeToInsert>,
    unique_rule_check: HashMap<String, u32>,
    read_write_buffer_objects: ReadWriteBufferObjects,
    processed_envs: HashSet<EnvDescriptor>,
}
pub struct NodeStatements<'a> {
    insert_node: SqlStatement<'a>,
    find_node: SqlStatement<'a>,
    update_mtime: SqlStatement<'a>,
    update_display_str: SqlStatement<'a>,
    update_flags: SqlStatement<'a>,
    update_srcid: SqlStatement<'a>,
    find_dir_id_with_par: SqlStatement<'a>,
    //find_nodeid: SqlStatement<'a>,
    add_to_dirpathbuf: SqlStatement<'a>,
    update_type: SqlStatement<'a>,
    add_to_tuppathbuf: SqlStatement<'a>,
    remove_tuppathbuf: SqlStatement<'a>,
    remove_dirpathbuf: SqlStatement<'a>,
}
pub(crate) struct AddIdsStatements<'a> {
    add_to_present: SqlStatement<'a>,
    add_to_modify: SqlStatement<'a>,
    add_to_delete: SqlStatement<'a>,
}

impl Collector {
    pub(crate) fn new(read_write_buffer_objects: ReadWriteBufferObjects) -> Result<Collector> {
        Ok(Collector {
            processed_globs: HashSet::new(),
            processed: HashSet::new(),
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
    fn collect_output(&mut self, p: &PathDescriptor, srcid: RuleDescriptor) -> Result<()> {
        self.nodes_to_insert
            .push(NodeToInsert::GeneratedFile(p.clone(), srcid));
        Ok(())
    }

    pub(crate) fn add_output(&mut self, p0: &PathDescriptor, id: RuleDescriptor) -> Result<()> {
        if self.processed_outputs.insert(p0.clone()) {
            self.collect_output(p0, id)
        } else {
            Err(eyre!(
                "output already processed: {:?}",
                p0.get_file_name().to_string()
            ))
        }
    }

    fn collect_glob(&mut self, p: &GlobPathDescriptor) -> Result<()> {
        self.nodes_to_insert.push(NodeToInsert::Glob(p.clone()));
        Ok(())
    }

    fn collect_excluded(&mut self, p: &PathDescriptor) -> Result<()> {
        self.nodes_to_insert
            .push(NodeToInsert::ExcludedFile(p.clone()));
        Ok(())
    }

    fn collect_input(&mut self, p: &PathDescriptor) -> Result<()> {
        log::debug!("collecting input: {:?}", p.get_file_name().to_string());
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

    fn add_input_for_insert(&mut self, resolved_input: &InputResolvedType) -> Result<()> {
        if let Some(p) = resolved_input.get_glob_path_desc() {
            if self.processed_globs.insert(p.clone()) && resolved_input.is_glob_match() {
                self.collect_glob(&p)?;
            }
        }
        if let Some(p) = resolved_input.get_resolved_path_desc() {
            if self.processed.insert(p.clone()) {
                self.collect_input(&p)?;
            }
        }
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
        let tuppath = task_instance.get_path().get_parent();
        let tuppathstr = tuppath.as_path();
        let line = task_instance.get_tup_loc().get_line();
        debug!(
            " task to insert: {} at  {}:{}",
            name,
            tuppathstr.display(),
            line
        );
        let prevline = self
            .unique_rule_check
            .insert(task_instance.get_path().to_string(), line);
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
        let tuppath = rule_formula.get_path().get_parent();
        {
            let line = rule_formula.get_rule_ref().get_line();
            if log::log_enabled!(log::Level::Debug) {
                let tuppathstr = tuppath.as_path();
                debug!(
                    " rule to insert: {} at  {}:{}",
                    name,
                    tuppathstr.display(),
                    line
                );
            }
            let prevline = self
                .unique_rule_check
                .insert(dir.to_string() + "/" + name.as_str(), line);
            if prevline.is_none() {
                self.collect_rule(rule_desc);
            } else {
                bail!(
                    "Rule at  {}:{} was previously defined at line {}. \
                                        Ensure that rule definitions take the inputs as arguments.",
                    tuppath.to_string().as_str(),
                    line,
                    prevline.unwrap()
                );
            }
        }
        Ok(())
    }

    fn add_groups(&mut self) -> Result<()> {
        self.read_write_buffer_objects.for_each_group(|grp_id| {
            self.nodes_to_insert
                .push(NodeToInsert::Group(grp_id.clone()));
            Ok(())
        })?;
        Ok(())
    }
}

fn insert_nodes(
    conn: &mut Connection,
    read_write_buf: &ReadWriteBufferObjects,
    resolved_rules: &ResolvedRules,
    crossref: &mut CrossRefMaps,
) -> Result<BTreeSet<(i64, RowType)>> {
    //let rules_in_tup_file = resolved_rules.rules_by_tup();

    let mut nodeids = BTreeSet::new();
    //let mut paths_to_update: HashMap<i64, i64> = HashMap::new();  we dont update nodes until rules are executed.
    let mut envs_to_insert = HashSet::new();

    let get_dir = |tup_desc: &TupPathDescriptor, crossref: &CrossRefMaps| -> Result<i64> {
        let (_, dir) = crossref.get_tup_db_id(tup_desc).ok_or_else(|| {
            eyre::Error::msg(format!(
                "No tup directory found in db for tup descriptor:{:?}",
                tup_desc
            ))
        })?;
        Ok(dir)
    };
    // collect all un-added groups and add them in a single transaction.
    let nodes: Vec<_> = {
        let mut collector = Collector::new(read_write_buf.clone())?;
        collector.add_groups()?;
        for resolvedtasks in resolved_rules.tasks_by_tup().iter() {
            for resolvedtask in resolvedtasks.iter() {
                let rd = resolvedtask.get_task_descriptor();
                for s in resolvedtask.get_deps() {
                    collector.add_input_for_insert(s)?;
                }
                collector.add_task_node(rd)?;
                let env_desc = resolvedtask.get_env_list();
                envs_to_insert.extend(env_desc.iter());
            }
        }

        for resolved_links in resolved_rules.rules_by_tup().iter() {
            for rl in resolved_links.iter() {
                let rd = rl.get_rule_desc();
                let rule_ref = rl.get_tup_loc();
                let tup_desc = rule_ref.get_tupfile_desc();
                let dir = get_dir(&tup_desc, &crossref)?;
                collector.add_rule_node(rd, dir)?;
                for p in rl.get_targets() {
                    collector.add_output(p, rl.get_rule_desc().clone())?
                }
                for s in rl.get_sources() {
                    collector.add_input_for_insert(s)?;
                }
                for p in rl.get_excluded_targets() {
                    collector.add_excluded(p.clone())?
                }
                for env in rl.get_env_list().iter() {
                    collector.add_env(&env);
                }
                for tupfiles_read in rl.get_tupfiles_read() {
                    collector.add_tupfile(&tupfiles_read)?;
                }
            }
        }
        collector.nodes()
    };

    let tx = conn.transaction()?;
    {
        let mut node_statements = NodeStatements::new(tx.deref())?;
        let mut add_ids_statements = AddIdsStatements::new(tx.deref())?;
        let parent_descriptors = nodes.iter().map(|n| n.get_parent_id(read_write_buf));
        // make sure directories are inserted before files
        for parent_desc in parent_descriptors {
            if !parent_desc.is_root() && crossref.get_path_db_id(&parent_desc).is_none() {
                let (parid, parparid) = insert_path(
                    read_write_buf.get_path(&parent_desc).as_path(),
                    &mut node_statements,
                    &mut add_ids_statements,
                    true,
                )?;
                if !parid.is_negative() {
                    crossref.add_path_xref(parent_desc, parid, parparid);
                }
            }
        }
        for node_to_insert in &nodes {
            let node = node_to_insert.get_node(&read_write_buf, crossref)?;
            let (db_id, db_par_id) = {
                let upsnode =
                    find_upsert_node(&mut node_statements, &mut add_ids_statements, &node)?;
                (upsnode.get_id(), upsnode.get_dir())
            };
            if !db_id.is_negative() {
                nodeids.insert((db_id, *node.get_type()));
                node_to_insert.update_crossref(crossref, db_id, db_par_id);
            }
        }
        let mut inst_env_stmt = tx.insert_env_prepare()?;
        let mut fetch_env_stmt = tx.fetch_env_id_prepare()?;
        let mut update_env_stmt = tx.update_env_prepare()?;
        let mut add_to_modify_env_stmt = tx.add_to_modify_prepare()?;
        for env_var in envs_to_insert.iter() {
            let env_val = env_var.get().get_val_str();
            let key = env_var.get().get_key_str();
            if let Ok((env_id, env_val_db)) = fetch_env_stmt.fetch_env_id(key) {
                if env_val_db.as_str().ne(env_val) {
                    update_env_stmt.update_env_exec(env_id, env_val)?;
                    add_to_modify_env_stmt.add_to_modify_exec(env_id, Env)?;
                }
                crossref.add_env_xref(env_var.clone(), env_id);
                nodeids.insert((env_id, Env));
            } else {
                let env_id = inst_env_stmt.insert_env_exec(key, env_val)?;
                crossref.add_env_xref(env_var.clone(), env_id);
                nodeids.insert((env_id, Env));
                add_to_modify_env_stmt.add_to_modify_exec(env_id, Env)?;
            }
        }
        // keep delete list node ids updated by removing output nodes that are alive
        for n in nodeids.iter() {
            tx.remove_id_from_delete_list(n.0)?;
        }
        tx.enrich_modified_list()?;
        tx.prune_present_list()?; // removes deletelist entries from present
        tx.prune_modified_list()?; // removes deletelist entries from modified
                                   //conn.remove_presents_prepare()?.remove_presents_exec()?;
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
            .flat_map(|rl| rl.get_sources().map(|s| (rl.get_tup_loc(), s)))
    };
    let srcs_from_tasks = {
        log::info!(
            "Cross referencing task inputs to insert with the db ids with same name and directory"
        );
        tasks
            .iter()
            .flat_map(|rl| rl.iter())
            .flat_map(|rl| rl.get_deps().iter().map(|s| (rl.get_tup_loc(), s)))
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
            //let node_id = node_statements.fetch_node_id(pd.get_file_name().as_ref(), dir);
            let (_, _) = crossref
                .get_path_db_id(pd)
                .ok_or_else(|| eyre!("path not found:{:?} mentioned in rule {:?}", s, i))?;
        }
    }

    Ok(nodeids)
}

impl NodeStatements<'_> {
    pub fn new(conn: &Connection) -> Result<NodeStatements> {
        let insert_node = conn.insert_node_prepare()?;
        let find_node = conn.fetch_node_prepare()?;
        let update_mtime = conn.update_mtime_prepare()?;
        let update_display_str = conn.update_display_str_prepare()?;
        let update_flags = conn.update_flags_prepare()?;
        let update_srcid = conn.update_srcid_prepare()?;
        let find_dir_id_with_par = conn.fetch_dirid_with_par_prepare()?;
        //let find_nodeid = conn.fetch_nodeid_prepare()?;
        let add_to_dirpathbuf = conn.insert_dir_aux_prepare()?;
        let update_type = conn.update_type_prepare()?;
        let add_to_tuppathbuf = conn.insert_tup_aux_prepare()?;
        let remove_tuppathbuf = conn.remove_tup_aux_prepare()?;
        let remove_dirpathbuf = conn.remove_dir_aux_prepare()?;

        Ok(NodeStatements {
            insert_node,
            find_node,
            update_mtime,
            update_display_str,
            update_flags,
            update_srcid,
            find_dir_id_with_par,
            // find_nodeid,
            add_to_dirpathbuf,
            update_type,
            add_to_tuppathbuf,
            remove_tuppathbuf,
            remove_dirpathbuf,
        })
    }
    fn insert_node_exec(&mut self, n: &Node) -> crate::db::Result<i64> {
        self.insert_node.insert_node_exec(n)
    }
    fn fetch_node(&mut self, name: &str, dirid: i64) -> crate::db::Result<Node> {
        self.find_node.fetch_node(name, dirid)
    }
    fn update_mtime_exec(&mut self, nodeid: i64, mtime: i64) -> crate::db::Result<()> {
        self.update_mtime.update_mtime_exec(nodeid, mtime)
    }

    fn update_type(&mut self, nodeid: i64, row_type: RowType) -> crate::db::Result<()> {
        self.update_type.update_type_exec(nodeid, row_type)
    }

    fn update_display_str_exec(&mut self, nodeid: i64, display_str: &str) -> crate::db::Result<()> {
        self.update_display_str
            .update_display_str(nodeid, display_str)
    }
    fn update_flags_exec(&mut self, nodeid: i64, flags: &str) -> crate::db::Result<()> {
        self.update_flags.update_flags_exec(nodeid, flags)
    }
    fn update_srcid_exec(&mut self, nodeid: i64, srcid: i64) -> crate::db::Result<()> {
        self.update_srcid.update_srcid_exec(nodeid, srcid)
    }

    /* #[allow(dead_code)]
    fn fetch_node_id(&mut self, name: &str, dirid: i64) -> crate::db::Result<i64> {
        self.find_nodeid.fetch_node_id(name, dirid)
    }*/
    fn fetch_dirid_with_par(&mut self, parent: &Path) -> crate::db::Result<(i64, i64)> {
        self.find_dir_id_with_par.fetch_dirid_with_par(parent)
    }

    fn add_to_dirpathbuf(&mut self, nodeid: i64, dirp: &Path) -> crate::db::Result<()> {
        self.add_to_dirpathbuf.insert_dir_aux_exec(nodeid, dirp)
    }
    fn add_to_tuppathbuf(&mut self, nodeid: i64, dirp: &Path) -> crate::db::Result<()> {
        self.add_to_tuppathbuf.insert_tup_aux_exec(nodeid, dirp)
    }
    fn remove_tuppathbuf(&mut self, nodeid: i64) -> crate::db::Result<()> {
        self.remove_tuppathbuf.remove_tup_aux_exec(nodeid)
    }
    fn remove_dirpathbuf(&mut self, nodeid: i64) -> crate::db::Result<()> {
        self.remove_dirpathbuf.remove_dir_aux_exec(nodeid)
    }
}

impl AddIdsStatements<'_> {
    pub fn new(conn: &Connection) -> Result<AddIdsStatements> {
        let add_to_present = conn.add_to_present_prepare()?;
        let add_to_modify = conn.add_to_modify_prepare()?;
        let add_to_delete = conn.add_to_delete_prepare()?;
        Ok(AddIdsStatements {
            add_to_present,
            add_to_modify,
            add_to_delete,
        })
    }
    fn add_to_modify(&mut self, nodeid: i64, rowtype: RowType) -> crate::db::Result<()> {
        self.add_to_modify.add_to_modify_exec(nodeid, rowtype)
    }
    fn add_to_present(&mut self, nodeid: i64, rowtype: RowType) -> crate::db::Result<()> {
        self.add_to_present.add_to_present_exec(nodeid, rowtype)
    }
    fn add_to_delete(&mut self, nodeid: i64, row_type: RowType) -> crate::db::Result<()> {
        self.add_to_delete.add_to_delete_exec(nodeid, row_type)
    }
}

/// [find_upsert_node] pretends to be the sqlite upsert operation
/// it also adds the node to the present list, modify list and updates nodes mtime
pub(crate) fn find_upsert_node(
    node_statements: &mut NodeStatements,
    add_ids_statements: &mut AddIdsStatements,
    node: &Node,
) -> Result<Node> {
    /*debug!(
        "find_upsert_node:{} in dir:{}",
        node.get_name(),
        node.get_dir()
    );*/
    let db_node = node_statements
        .fetch_node(node.get_name(), node.get_dir())
        .and_then(|existing_node| {
            update_columns(node_statements, add_ids_statements, node, &existing_node)?;
            Ok(existing_node)
        })
        .or_else(|e| {
            if e.has_no_rows() {
                let node = node_statements
                    .insert_node_exec(node)
                    .map(|i| Node::copy_from(i, node))?;
                add_ids_statements.add_to_modify(node.get_id(), *node.get_type())?;
                add_ids_statements.add_to_present(node.get_id(), *node.get_type())?; // add to present list
                Ok::<Node, eyre::Error>(node)
            } else {
                Err::<Node, eyre::Error>(e.into())
            }
        })?;
    Ok(db_node)
}

fn update_columns(
    node_statements: &mut NodeStatements,
    add_ids_statements: &mut AddIdsStatements,
    node: &Node,
    existing_node: &Node,
) -> Result<(), AnyError> {
    let mut modify = false;
    // verify that columns of this row are the same as `node`'s, if not update them
    if existing_node.get_type().ne(&node.get_type()) {
        if is_generated(existing_node.get_type()) {
            log::debug!(
                "Keeping row type of generated file: {} until no rule writes to it",
                existing_node.get_name()
            );
        } else {
            debug!(
                "update type for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_type(),
                node.get_type()
            );
            node_statements.update_type(existing_node.get_id(), *node.get_type())?;
            modify = true;
        }
    }
    if (existing_node.get_mtime() - node.get_mtime()).abs() > 1 {
        debug!(
            "updating mtime for:{}, {} -> {}",
            existing_node.get_name(),
            existing_node.get_mtime(),
            node.get_mtime()
        );
        node_statements.update_mtime_exec(existing_node.get_id(), node.get_mtime())?;
        modify = true;
    }
    if existing_node.get_display_str() != node.get_display_str() {
        debug!(
            "updating display_str for:{}, {} -> {}",
            existing_node.get_name(),
            existing_node.get_display_str(),
            node.get_display_str()
        );
        node_statements.update_display_str_exec(existing_node.get_id(), node.get_display_str())?;
        modify = true;
    }
    if existing_node.get_flags() != node.get_flags() {
        debug!(
            "updating flags for:{}, {} -> {}",
            existing_node.get_name(),
            existing_node.get_flags(),
            node.get_flags()
        );
        node_statements.update_flags_exec(existing_node.get_id(), node.get_flags())?;
        modify = true;
    }
    if existing_node.get_srcid() != node.get_srcid() {
        debug!(
            "updating srcid for:{}, {} -> {}",
            existing_node.get_name(),
            existing_node.get_srcid(),
            node.get_srcid()
        );
        node_statements.update_srcid_exec(existing_node.get_id(), node.get_srcid())?;
        modify = true;
    }
    if modify {
        add_ids_statements.add_to_modify(existing_node.get_id(), *node.get_type())?;
    }
    add_ids_statements.add_to_present(existing_node.get_id(), *existing_node.get_type())?;
    Ok(())
}

fn is_generated(p0: &RowType) -> bool {
    p0 == &GenF || p0 == &DirGen
}

/// add links from targets that contribute to a group to the group id
fn add_group_providers(
    conn: &mut Connection,
    resolved_rules: &ResolvedRules,
    crossref: &CrossRefMaps,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut inp_linker = tx.insert_link_prepare()?;

        for (group_id, targets) in resolved_rules
            .get_resolved_links()
            .iter()
            .filter_map(|rl| rl.get_group_desc().as_ref().map(|g| (*g, rl.get_targets())))
        // filter those that have a group descriptor
        {
            let (group_db_id, _) = crossref
                .get_group_db_id(group_id)
                .ok_or_else(|| eyre!("group db id not found for group: {:?}", group_id))?;
            for target in targets {
                let (path_db_id, _) = crossref
                    .get_path_db_id(target)
                    .ok_or_else(|| eyre!("path db id not found for target: {:?}", target))?;
                inp_linker.insert_link(path_db_id, group_db_id, false, RowType::Grp)?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}

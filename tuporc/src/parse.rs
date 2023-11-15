use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use bimap::BiMap;
use crossbeam::sync::WaitGroup;
use eyre::{bail, eyre, Report, Result};
use log::debug;
use parking_lot::Mutex;
use rusqlite::Connection;

use tupparser::buffers::{
    GlobPathDescriptor, GroupPathDescriptor, OutputHolder, PathBuffers, PathDescriptor,
    RuleDescriptor, TaskDescriptor, TupPathDescriptor,
};
use tupparser::decode::{OutputHandler, PathSearcher};
use tupparser::errors::Error;
use tupparser::paths::{GlobPath, InputResolvedType, MatchingPath, NormalPath};
use tupparser::{Artifacts, ReadWriteBufferObjects, TupParser};

use crate::db::RowType::{Env, Excluded, GenF, Glob, Rule};
use crate::db::{
    create_path_buf_temptable, AnyError, ForEachClauses, LibSqlExec, MiscStatements, SqlStatement,
};
use crate::scan::{get_dir_id, MAX_THRS_DIRS};
use crate::{LibSqlPrepare, Node, RowType, TermProgress};

// CrossRefMaps maps paths, groups and rules discovered during parsing with those found in database
// These are two ways maps, so you can query both ways
#[derive(Debug, Clone, Default)]
pub struct CrossRefMaps {
    gbo: BiMap<GroupPathDescriptor, (i64, i64)>,
    // group id and the corresponding db id,, parent id
    pbo: BiMap<PathDescriptor, (i64, i64)>,
    // path id and the corresponding db id, parent id (includes globs)
    rbo: BiMap<RuleDescriptor, (i64, i64)>,
    // rule id and the corresponding db id, parent id
    dbo: BiMap<TupPathDescriptor, (i64, i64)>,
    // tup id and the corresponding db id, parent id
    ebo: BiMap<String, i64>, // env id and the corresponding db id
    // task id and the corresponding db id, parent id
    tbo: BiMap<TaskDescriptor, (i64, i64)>,
}

impl CrossRefMaps {
    pub fn get_group_db_id(&self, g: &GroupPathDescriptor) -> Option<(i64, i64)> {
        self.gbo.get_by_left(g).copied()
    }
    pub fn get_path_db_id(&self, p: &PathDescriptor) -> Option<(i64, i64)> {
        self.pbo.get_by_left(p).copied()
    }
    pub fn get_rule_db_id(&self, r: &RuleDescriptor) -> Option<(i64, i64)> {
        self.rbo.get_by_left(r).copied()
    }

    pub fn get_tup_db_id(&self, r: &TupPathDescriptor) -> Option<(i64, i64)> {
        self.dbo.get_by_left(r).copied()
    }

    pub fn get_env_db_id(&self, e: &String) -> Option<i64> {
        self.ebo.get_by_left(e).copied()
    }

    pub fn get_glob_db_id(&self, s: &GlobPathDescriptor) -> Option<(i64, i64)> {
        let p = PathDescriptor::new((*s).into());
        self.pbo.get_by_left(&p).copied()
    }

    pub fn get_task_id(&self, t: &TaskDescriptor) -> Option<(i64, i64)> {
        self.tbo.get_by_left(t).copied()
    }

    pub fn add_group_xref(&mut self, g: GroupPathDescriptor, db_id: i64, par_db_id: i64) {
        self.gbo.insert(g, (db_id, par_db_id));
    }
    pub fn add_env_xref(&mut self, e: String, db_id: i64) {
        self.ebo.insert(e, db_id);
    }

    pub fn add_path_xref(&mut self, p: PathDescriptor, db_id: i64, par_db_id: i64) {
        self.pbo.insert(p, (db_id, par_db_id));
    }
    pub fn add_rule_xref(&mut self, r: RuleDescriptor, db_id: i64, par_db_id: i64) {
        self.rbo.insert(r, (db_id, par_db_id));
    }
    pub fn add_tup_xref(&mut self, t: TupPathDescriptor, db_id: i64, par_db_id: i64) {
        self.dbo.insert(t, (db_id, par_db_id));
    }
    pub fn add_task_xref(&mut self, t: TaskDescriptor, db_id: i64, par_db_id: i64) {
        self.tbo.insert(t.into(), (db_id, par_db_id));
    }
}

/// Path searcher that scans the sqlite database for matching paths
/// It is used by the parser to resolve paths and globs. Resolved outputs are dumped in OutputHolder
#[derive(Debug, Clone)]
struct DbPathSearcher {
    conn: Arc<Mutex<Connection>>,
    psx: OutputHolder,
}

impl DbPathSearcher {
    pub fn new(conn: Connection) -> DbPathSearcher {
        DbPathSearcher {
            conn: Arc::new(Mutex::new(conn)),
            psx: OutputHolder::new(),
        }
    }

    fn fetch_glob_nodes(
        &self,
        ph: &mut impl PathBuffers,
        glob_paths: &[GlobPath], // glob paths to search for (corresponding to search dirs that were specified before)
    ) -> std::result::Result<Vec<MatchingPath>, AnyError> {
        for glob_path in glob_paths {
            let has_glob_pattern = glob_path.has_glob_pattern();
            if has_glob_pattern {
                debug!(
                    "looking for matches in db for glob pattern: {:?}",
                    glob_path.get_abs_path()
                );
            }

            let base_path = glob_path.get_base_abs_path();
            if base_path.is_dir() {
                debug!("base path is dir: {:?}", base_path);
            } else {
                debug!("base path is not dir: {:?}", base_path);
                continue;
            }
            let glob_pattern = ph.get_rel_path(&glob_path.get_glob_path_desc(), base_path);
            let fetch_row = |s: &String| -> Option<MatchingPath> {
                debug!("found:{} at {:?}", s, base_path);
                let (pd, _) = ph.add_path_from(base_path, Path::new(s.as_str()));
                if has_glob_pattern {
                    let full_path = glob_path.get_base_abs_path().join(s.as_str());

                    if glob_path.is_match(full_path.as_path()) {
                        let grps = glob_path.group(full_path.as_path());
                        debug!("found match: {:?} for {:?}", grps, glob_pattern);
                        Some(MatchingPath::with_captures(
                            pd,
                            NormalPath::absolute_from(
                                Path::new(s.as_str()),
                                glob_path.get_base_abs_path(),
                            ),
                            glob_path.get_glob_desc(),
                            grps,
                        ))
                    } else {
                        None
                    }
                } else {
                    Some(MatchingPath::new(pd, ph.get_path(&pd).clone()))
                }
            };
            let conn = self.conn.deref().lock();
            let recursive = glob_path.is_recursive_prefix();

            let mut glob_query = conn.fetch_glob_nodes_prepare(recursive)?;

            let mps = glob_query.fetch_glob_nodes(
                base_path,
                glob_pattern.as_path(),
                recursive,
                fetch_row,
            );
            match mps {
                Ok(mps) => {
                    if !mps.is_empty() {
                        return Ok(mps);
                    }
                }
                Err(e) if e.has_no_rows() => {
                    debug!("no rows found for glob pattern: {:?}", glob_pattern);
                }
                Err(e) => return Err(e),
            }
        }
        return Err(AnyError::Db(rusqlite::Error::QueryReturnedNoRows));
    }
}

impl PathSearcher for DbPathSearcher {
    fn discover_paths(
        &self,
        path_buffers: &mut impl PathBuffers,
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
            x.cmp(y)
        };
        mps.sort_by(c);
        mps.dedup();
        Ok(mps)
    }
    fn get_outs(&self) -> &OutputHolder {
        &self.psx
    }

    fn merge(&mut self, p: &impl PathBuffers, o: &impl OutputHandler) -> Result<(), Error> {
        OutputHandler::merge(&mut self.psx, p, o)
    }
}

/// handle the tup parse command which assumes files in db and adds rules and makes links joining input and output to/from rule statements
pub(crate) fn parse_tupfiles_in_db<P: AsRef<Path>>(
    connection: Connection,
    tupfiles: Vec<Node>,
    root: P,
    mut term_progress: TermProgress,
) -> Result<()> {
    let mut crossref = CrossRefMaps::default();
    term_progress = term_progress.set_main_with_len("Parsing tupfiles", 2 * tupfiles.len() as u64);
    let (arts, mut rwbufs, mut outs) = {
        let conn = connection;

        let db = DbPathSearcher::new(conn);
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
        let arts = gather_rules_from_tupfiles(&mut parser, tupfiles.as_slice(), &term_progress)?;
        let arts = parser.reresolve(arts)?;
        for tup_node in tupfiles.iter() {
            let tupid = *parser
                .read_write_buffers()
                .get_tup_id(Path::new(tup_node.get_name()));
            crossref.add_tup_xref(tupid, tup_node.get_id(), tup_node.get_dir());
        }
        (arts, parser.read_write_buffers(), parser.get_outs().clone())
    };
    let mut conn = Connection::open(".tup/db")
        .expect("Connection to tup database in .tup/db could not be established");

    let _ = insert_nodes(&mut conn, &rwbufs, &arts, &mut crossref)?;

    check_uniqueness_of_parent_rule(&mut conn, &rwbufs, &outs, &mut crossref)?;

    add_links_to_groups(&mut conn, &arts, &crossref)?;
    fetch_group_providers(&mut conn, &mut rwbufs, &mut outs, &mut crossref)?;
    add_rule_links(&mut conn, &rwbufs, &arts, &mut crossref)?;
    // add links from glob inputs to tupfiles's directory
    add_link_glob_dir_to_rules(&mut conn, &rwbufs, &arts, &mut crossref)?;
    Ok(())
}

/// adds links from glob patterns specified at each directory that are inputs to rules  to the tupfile directory
/// We dont directly add links from glob patterns to rules, because already have resolved the glob patterns to paths in a previous iterations of parsing.
/// Newer / modified /deleted inputs discovered in glob patterns and added as rule inputs will be process in a  re-iteration parsing phase of Tupfile which the glob pattern links to
fn add_link_glob_dir_to_rules(
    conn: &mut Connection,
    rw_buf: &ReadWriteBufferObjects,
    arts: &Artifacts,
    crossref: &mut CrossRefMaps,
) -> Result<(), Report> {
    let tx = conn.transaction()?;
    {
        let mut insert_link = tx.insert_link_prepare()?;
        for rlink in arts.get_resolved_links() {
            let tupfile_desc = rlink.get_tup_loc().get_tupfile_desc();
            let (tupfile_db_id, _) = crossref.get_tup_db_id(tupfile_desc).ok_or_else(|| {
                eyre!(
                    "tupfile dir not found:{:?} mentioned in rule {:?}",
                    tupfile_desc,
                    rlink.get_tup_loc()
                )
            })?;

            rlink.for_each_glob_path_desc(|glob_path_desc| -> Result<(), Error> {
                //               links_to_add.insert(glob_path_desc, tupfile_db_id);
                let (glob_pattern_id, _) =
                    crossref.get_glob_db_id(&glob_path_desc).ok_or_else(|| {
                        Error::new_path_search_error(format!(
                            "glob path not found:{:?} in tup db",
                            rw_buf.get_glob_path(&glob_path_desc)
                        ))
                    })?;
                insert_link
                    .insert_link(glob_pattern_id, tupfile_db_id, true, RowType::TupF)
                    .map_err(|e| Error::new_path_search_error(e.to_string()))?;
                Ok(())
            })?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub fn gather_tupfiles(conn: &mut Connection) -> Result<Vec<Node>> {
    let mut tupfiles = Vec::new();
    create_path_buf_temptable(conn)?;

    conn.for_changed_or_created_tup_node_with_path(|n: Node| {
        // name stores full path here
        tupfiles.push(n);
        Ok(())
    })?;
    Ok(tupfiles)
}

fn gather_rules_from_tupfiles(
    p: &mut TupParser<DbPathSearcher>,
    tupfiles: &[Node],
    term_progress: &TermProgress,
) -> Result<Artifacts> {
    //let mut del_stmt = conn.delete_tup_rule_links_prepare()?;
    let mut new_arts = Artifacts::new();
    let (sender, receiver) = crossbeam::channel::unbounded();
    term_progress.set_message("Parsing Tupfiles");
    crossbeam::thread::scope(|s| -> Result<Artifacts> {
        let wg = WaitGroup::new();
        for ithread in 0..MAX_THRS_DIRS {
            let mut p = p.clone();
            let sender = sender.clone();
            let wg = wg.clone();
            let pb = term_progress.make_progress_bar("_");
            s.spawn(move |_| -> Result<()> {
                for tupfile in tupfiles
                    .iter()
                    .filter(|x| !x.get_name().ends_with(".lua"))
                    .cloned()
                    .skip(ithread as usize)
                    .step_by(MAX_THRS_DIRS as usize)
                {
                    pb.set_message(format!("Parsing :{}", tupfile.get_name()));
                    p.parse_tupfile(tupfile.get_name(), sender.clone())
                        .map_err(|error| {
                            let rwbuf = p.read_write_buffers();
                            let display_str = rwbuf.display_str(&error);
                            let tupfile_name = tupfile.get_name();
                            term_progress.abandon(&pb, format!("Error parsing {tupfile_name}"));
                            eyre!(
                                "Error while parsing tupfile: {}:\n {} due to \n{}",
                                tupfile.get_name(),
                                display_str,
                                error
                            )
                        })?;
                    pb.set_message(format!("Done parsing {}", tupfile.get_name()));
                    term_progress.tick(&pb);
                }
                drop(wg);
                pb.set_message("Done");
                Ok(())
            });
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
            let arts = p.parse(path).map_err(|ref e| {
                term_progress.abandon(&pb, format!("Error parsing {path}"));
                eyre!("Error: {}", p.read_write_buffers().display_str(e))
            })?;
            new_arts.extend(arts);
            term_progress.tick(&pb);
            pb.set_message(format!("Done parsing {}", path));
        }
        pb.set_message("Resolving statements..");
        new_arts.extend(p.receive_resolved_statements(receiver).map_err(|error| {
            let read_write_buffers = p.read_write_buffers();
            let tup_node = read_write_buffers
                .get_tup_path(error.get_tup_descriptor())
                .to_string_lossy()
                .to_string();
            let display_str = read_write_buffers.display_str(error.get_error_ref());
            term_progress.abandon(&pb, format!("Error parsing {tup_node}"));
            eyre!(
                "Unable to resolve statements in tupfile {}:\n{}",
                tup_node.as_str(),
                display_str,
            )
        })?);
        term_progress.finish(&pb, "Done parsing tupfiles");
        term_progress.clear();
        wg.wait();
        Ok(new_arts)
    })
    .expect("Threading error while fetching artifacts from tupfiles")
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
                                node.get_name(), tup_path.to_string_lossy(), parent_rule_ref.get_line()
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
    arts: &Artifacts,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let rules_in_tup_file = arts.rules_by_tup();
    let tconn = conn.transaction()?;
    {
        let mut inp_linker = tconn.insert_link_prepare()?;
        let mut out_linker = tconn.insert_link_prepare()?;
        for r in rules_in_tup_file {
            for rl in r {
                let (rule_node_id, _) = crossref
                    .get_rule_db_id(rl.get_rule_desc())
                    .expect("rule dbid fetch failed");
                let mut processed = std::collections::HashSet::new();
                let mut processed_group = std::collections::HashSet::new();
                let env_desc = rl.get_env_desc();
                let environs = rbuf.get_envs(env_desc);
                log::info!(
                    "adding links from envs  {:?} to rule: {:?}",
                    env_desc,
                    rule_node_id
                );
                for env_var in environs.get_keys() {
                    let env_id = crossref.get_env_db_id(&env_var).ok_or_else(|| {
                        eyre!("database env id not found for env var: {}", env_var)
                    })?;
                    inp_linker.insert_link(env_id, rule_node_id, false, RowType::Rule)?;
                }
                log::info!(
                    "adding links from inputs  {:?} to rule: {:?}",
                    rl.get_sources(),
                    rule_node_id
                );
                for i in rl.get_sources() {
                    match i {
                        InputResolvedType::UnResolvedGroupEntry(g) => {
                            let (group_id, _) = crossref.get_group_db_id(&g).ok_or_else(|| {
                                eyre!("db group not found with descriptor {:?}", g)
                            })?;
                            inp_linker.insert_link(group_id, rule_node_id, true, RowType::Rule)?;
                        }
                        InputResolvedType::Deglob(mp) => {
                            let (pid, _) = crossref
                                .get_path_db_id(mp.path_descriptor())
                                .ok_or_else(|| {
                                    eyre!(
                                        "db path not found with descriptor {:?} => {:?}",
                                        mp.path_descriptor(),
                                        rbuf.get_input_path_str(&i)
                                    )
                                })?;
                            debug!("slink {} => {}", pid, rule_node_id);
                            if processed.insert(pid) {
                                inp_linker.insert_link(pid, rule_node_id, true, RowType::Rule)?;
                            }
                        }
                        InputResolvedType::BinEntry(_, p) => {
                            let (pid, _) = crossref
                                .get_path_db_id(&p)
                                .ok_or_else(|| eyre!("bin entry not found in db:{:?}", p))?;
                            debug!("bin slink {} => {}", pid, rule_node_id);
                            if processed.insert(pid) {
                                inp_linker.insert_link(pid, rule_node_id, true, RowType::Rule)?;
                            }
                        }
                        InputResolvedType::GroupEntry(g, p) => {
                            let (group_id, _) = crossref.get_group_db_id(&g).ok_or_else(|| {
                                eyre!("db group not found with descriptor {:?}", g)
                            })?;
                            debug!("group link {} => {}", group_id, rule_node_id);
                            if processed_group.insert(group_id) {
                                inp_linker.insert_link(
                                    group_id,
                                    rule_node_id,
                                    true,
                                    RowType::Rule,
                                )?;
                            }
                            if let Some((pid, _)) = crossref.get_path_db_id(&p) {
                                if processed.insert(pid) {
                                    inp_linker.insert_link(
                                        pid,
                                        rule_node_id,
                                        true,
                                        RowType::Rule,
                                    )?;
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
                            out_linker.insert_link(rule_node_id, p, true, RowType::GenF)?;
                        }
                    } else {
                        for t in rl.get_targets() {
                            let (p, _) = crossref
                                .get_path_db_id(t)
                                .ok_or_else(|| eyre!("failed to fetch db id of path {}", t))?;
                            out_linker.insert_link(rule_node_id, p, true, RowType::GenF)?;
                        }
                    }
                    for t in rl.get_excluded_targets() {
                        let (p, _) = crossref
                            .get_path_db_id(t)
                            .ok_or_else(|| eyre!("failed to fetch db id of path {}", t))?;
                        out_linker.insert_link(rule_node_id, p, true, RowType::Excluded)?;
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
    let vs = rwbuf.map_group_desc(|group_desc| -> (GroupPathDescriptor, i64) {
        (
            *group_desc,
            crossref
                .get_group_db_id(group_desc)
                .unwrap_or_else(|| {
                    panic!(
                        "could not fetch groupid from its internal id:{}",
                        group_desc
                    )
                })
                .0,
        )
    });

    for (group_desc, groupid) in vs {
        conn.for_each_grp_node_provider(
            groupid,
            Some(GenF),
            |node: Node| -> std::result::Result<(), AnyError> {
                // name of node is actually its path
                // merge providers of this group from all available in db
                let pd = rwbuf.add_abs(Path::new(node.get_name())).0;
                outs.add_group_entry(&group_desc, pd);
                Ok(())
            },
        )?;
    }
    Ok(())
}

fn find_by_path(
    path: &Path,
    find_stmt: &mut SqlStatement,
    fetch_dir_id: &mut SqlStatement,
) -> Result<(i64, i64, i64)> {
    let parent = path
        .parent()
        .ok_or_else(|| eyre!("No parent folder found for file {:?}", path))?;

    let (dir, parent_id) = fetch_dir_id.fetch_dirid_with_par(parent)?;
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .ok_or_else(|| eyre!("path is missing file name:{:?}", path))?;
    let i = find_stmt.fetch_node_id(name.as_str(), dir)?;
    Ok((i, dir, parent_id))
}

fn insert_nodes(
    conn: &mut Connection,
    read_write_buf: &ReadWriteBufferObjects,
    arts: &Artifacts,
    crossref: &mut CrossRefMaps,
) -> Result<BTreeSet<(i64, RowType)>> {
    //let rules_in_tup_file = arts.rules_by_tup();

    let mut groups_to_insert: Vec<_> = Vec::new();
    let mut paths_to_insert = BTreeSet::new();
    let mut rules_to_insert = Vec::new();
    let mut nodeids = BTreeSet::new();
    //let mut paths_to_update: HashMap<i64, i64> = HashMap::new();  we dont update nodes until rules are executed.
    let mut envs_to_insert = HashMap::new();

    // collect all un-added groups and add them in a single transaction.
    {
        let mut find_dirid = conn.fetch_dirid_prepare()?;
        let mut find_group_id = conn.fetch_groupid_prepare()?;
        read_write_buf.for_each_group(|(group_path, grp_id)| {
            let parent = tupparser::transform::get_parent_with_fsep(group_path.as_path());
            if let Some(dir) = get_dir_id(&mut find_dirid, parent.to_cow_str().as_ref()) {
                let grp_name = group_path.file_name();
                let id = find_group_id.fetch_group_id(grp_name.as_str(), dir).ok();
                if let Some(i) = id {
                    // grp_db_id.insert(grp_id, i);
                    crossref.add_group_xref(*grp_id, i, dir);
                    nodeids.insert((i, RowType::Grp));
                } else {
                    // gather groups that are not in the db yet.
                    let isz: usize = (*grp_id).into();
                    groups_to_insert.push(Node::new_grp(isz as i64, dir, grp_name));
                }
            }
        });
        let mut find_nodeid = conn.fetch_nodeid_prepare()?;
        let mut find_dir_id_with_parent = conn.fetch_dirid_with_par_prepare()?;

        let mut unique_rule_check = HashMap::new();

        let mut existing_nodeids = BTreeSet::new();
        let mut find_dirid_with_par = conn.fetch_dirid_with_par_prepare()?;
        let mut collect_nodes_to_insert = |p: &PathDescriptor,
                                           rtype: &RowType,
                                           mtime_ns: i64,
                                           srcid: i64,
                                           crossref: &mut CrossRefMaps,
                                           find_nodeid: &mut SqlStatement|
         -> Result<()> {
            let isz: usize = (*p).into();
            let path = read_write_buf.get_path(p);
            let parent = path
                .as_path()
                .parent()
                .ok_or_else(|| eyre!("No parent folder found for file {:?}", path.as_path()))?;
            let dir_desc = read_write_buf
                .get_parent_id(p)
                .ok_or_else(|| eyre!("descriptor not found for path:{:?}", parent))?;
            let (dir, par_dir) = {
                let x = find_dirid_with_par.fetch_dirid_with_par(parent);
                x.map_err(|e| eyre!("failed to fetch dir id of {:?} due to {}", parent, e))?
            };
            crossref.add_path_xref(dir_desc, dir, par_dir);

            let name = path
                .as_path()
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .ok_or_else(|| eyre!("missing name:{:?} for a path to insert", path.as_path()))?;
            if let Ok(nodeid) = find_nodeid.fetch_node_id(&name, dir) {
                //path_db_id.insert(p, nodeid);
                debug!("found {} in dir:{} to id:{}", name, dir, nodeid);
                crossref.add_path_xref(*p, nodeid, dir);
                // this is not to be upserted until actually written
                //paths_to_update.insert(nodeid, mtime_ns);
                existing_nodeids.insert((nodeid, *rtype));
            } else {
                debug!("need to add {:?} in dir:{}", name, dir);
                paths_to_insert.insert(Node::new_file_or_genf(
                    isz as i64, dir, mtime_ns, name, *rtype, srcid,
                ));
            }
            Ok(())
        };
        let mut processed = std::collections::HashSet::new();
        let mut processed_globs = std::collections::HashSet::new();
        {
            let mut collect_task_nodes_to_insert = |task_desc: &TaskDescriptor,
                                                    dir: i64,
                                                    crossref: &mut CrossRefMaps,
                                                    find_nodeid: &mut SqlStatement|
             -> Result<()> {
                let isz: usize = (*task_desc).into();
                let task_instance = read_write_buf.get_task(task_desc);
                let name = task_instance.get_target();
                let display_str = name.to_string();
                let flags = String::new();
                let srcid = u32::MAX;
                if let Ok(nodeid) = find_nodeid.fetch_node_id(name, dir) {
                    crossref.add_task_xref(*task_desc, nodeid, dir);
                    nodeids.insert((nodeid, RowType::Task));
                } else {
                    let tuppath =
                        read_write_buf.get_tup_path(task_instance.get_tup_loc().get_tupfile_desc());
                    let tuppathstr = tuppath.to_string_lossy();
                    let line = task_instance.get_tup_loc().get_line();
                    debug!(" task to insert: {} at  {}:{}", name, tuppathstr, line);
                    let prevline = unique_rule_check.insert(name.to_string(), line);
                    if prevline.is_none() {
                        rules_to_insert.push(Node::new_task(
                            isz as i64,
                            dir,
                            name.to_string(),
                            display_str,
                            flags,
                            srcid,
                        ));
                    } else {
                        bail!(
                            "Task at  {}:{} was previously defined at line {}. \
                        Ensure that rule definitions take the inputs as arguments.",
                            tuppathstr.to_string().as_str(),
                            line,
                            prevline.unwrap()
                        );
                    }
                }
                Ok(())
            };

            for r in arts.tasks_by_tup().iter() {
                for rl in r.iter() {
                    let rd = rl.get_task_descriptor();
                    let tup_desc = rl.get_tup_loc().get_tupfile_desc();
                    let (_, dir) = crossref.get_tup_db_id(tup_desc).ok_or_else(|| {
                        eyre::Error::msg(format!(
                            "No tup directory found in db for tup descriptor:{:?}",
                            tup_desc
                        ))
                    })?;
                    collect_task_nodes_to_insert(rd, dir, crossref, &mut find_nodeid)?;
                    for s in rl.get_deps() {
                        if let Some(p) = s.get_glob_path_desc() {
                            if processed_globs.insert(p) && s.is_glob_match() {
                                let p = PathDescriptor::new(p.into());
                                collect_nodes_to_insert(
                                    &p,
                                    &Glob,
                                    0,
                                    dir,
                                    crossref,
                                    &mut find_nodeid,
                                )?;
                            }
                        }
                    }
                    let env_desc = rl.get_env_desc();
                    let environs = read_write_buf.get_envs(&env_desc);
                    envs_to_insert.extend(environs.getenv());
                }
            }
        }
        {
            let mut collect_rule_nodes_to_insert = |rule_desc: &RuleDescriptor,
                                                    dir: i64,
                                                    crossref: &mut CrossRefMaps,
                                                    find_nodeid: &mut SqlStatement|
             -> Result<()> {
                let isz: usize = (*rule_desc).into();
                let rule_formula = read_write_buf.get_rule(rule_desc);
                let name = rule_formula.get_rule_str();
                let display_str = rule_formula.get_display_str();
                let flags = rule_formula.get_flags();
                let srcid = rule_formula.get_rule_ref().get_line();
                if let Ok(nodeid) = find_nodeid.fetch_node_id(name.as_str(), dir) {
                    crossref.add_rule_xref(*rule_desc, nodeid, dir);
                    nodeids.insert((nodeid, RowType::Rule));
                } else {
                    let tuppath =
                        read_write_buf.get_tup_path(rule_formula.get_rule_ref().get_tupfile_desc());
                    let tuppathstr = tuppath.to_string_lossy();
                    let line = rule_formula.get_rule_ref().get_line();
                    debug!(" rule to insert: {} at  {}:{}", name, tuppathstr, line);
                    let prevline =
                        unique_rule_check.insert(dir.to_string() + "/" + name.as_str(), line);
                    if prevline.is_none() {
                        rules_to_insert.push(Node::new_rule(
                            isz as i64,
                            dir,
                            name,
                            display_str,
                            flags,
                            srcid,
                        ));
                    } else {
                        bail!(
                            "Rule at  {}:{} was previously defined at line {}. \
                        Ensure that rule definitions take the inputs as arguments.",
                            tuppathstr.to_string().as_str(),
                            line,
                            prevline.unwrap()
                        );
                    }
                }
                Ok(())
            };
            for r in arts.rules_by_tup().iter() {
                for rl in r.iter() {
                    let rd = rl.get_rule_desc();
                    let rule_ref = rl.get_tup_loc();
                    let tup_desc = rule_ref.get_tupfile_desc();
                    let (_, dir) = crossref.get_tup_db_id(tup_desc).ok_or_else(|| {
                        eyre::Error::msg(format!(
                            "No tup directory found in db for tup descriptor:{:?}",
                            tup_desc
                        ))
                    })?;
                    collect_rule_nodes_to_insert(rd, dir, crossref, &mut find_nodeid)?;
                    for p in rl.get_targets() {
                        if processed.insert(p) {
                            collect_nodes_to_insert(p, &GenF, 0, dir, crossref, &mut find_nodeid)?;
                        }
                    }
                    for s in rl.get_sources() {
                        if let Some(p) = s.get_glob_path_desc() {
                            if processed_globs.insert(p) && s.is_glob_match() {
                                let p = PathDescriptor::new(p.into());
                                collect_nodes_to_insert(
                                    &p,
                                    &Glob,
                                    0,
                                    dir,
                                    crossref,
                                    &mut find_nodeid,
                                )?;
                            }
                        }
                    }
                    for p in rl.get_excluded_targets() {
                        if processed.insert(p) {
                            collect_nodes_to_insert(
                                p,
                                &Excluded,
                                0,
                                dir,
                                crossref,
                                &mut find_nodeid,
                            )?;
                        }
                    }
                    let env_desc = rl.get_env_desc();
                    let environs = read_write_buf.get_envs(env_desc);
                    envs_to_insert.extend(environs.getenv());
                }
            }
        }
        let rules = arts.rules_by_tup();
        let tasks = arts.tasks_by_tup();
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

        for (tup_loc, i) in srcs_from_links.chain(srcs_from_tasks) {
            let inp = read_write_buf.get_input_path_str(&i);
            let p = i.get_resolved_path_desc().ok_or_else(|| {
                eyre!(
                    "No resolved path found for input:{:?} in rule:{:?}",
                    inp,
                    tup_loc.clone()
                )
            })?;
            if processed.insert(p) {
                let (nodeid, dirid, par_dir_id) = find_by_path(
                    Path::new(inp.as_str()),
                    &mut find_nodeid,
                    &mut find_dir_id_with_parent,
                )
                .map_err(|e| eyre!("failed to find path for {:?} due to {}", inp, e))?;
                let parent_id = read_write_buf
                    .get_parent_id(p)
                    .ok_or_else(|| eyre!("no parent id found for:{:?}", p))?;
                crossref.add_path_xref(*p, nodeid, dirid);
                crossref.add_path_xref(parent_id, dirid, par_dir_id);
            }
        }
        nodeids.extend(existing_nodeids.iter());
    }
    let tx = conn.transaction()?;
    {
        let mut node_statements = NodeStatements::new(tx.deref())?;
        let mut add_ids_statements = AddIdsStatements::new(tx.deref())?;
        for node in groups_to_insert
            .into_iter()
            .chain(paths_to_insert.into_iter())
            .into_iter()
            .chain(rules_to_insert.into_iter())
        {
            let desc = node.get_id() as usize;
            let db_id =
                find_upsert_node(&mut node_statements, &mut add_ids_statements, &node)?.get_id();
            nodeids.insert((db_id, *node.get_type()));
            if RowType::Grp.eq(node.get_type()) {
                crossref.add_group_xref(GroupPathDescriptor::new(desc), db_id, node.get_dir());
            } else if Rule.eq(node.get_type()) {
                crossref.add_rule_xref(RuleDescriptor::new(desc), db_id, node.get_dir());
            } else {
                crossref.add_path_xref(PathDescriptor::new(desc), db_id, node.get_dir());
            }
        }
        let mut inst_env_stmt = tx.insert_env_prepare()?;
        let mut fetch_env_stmt = tx.fetch_env_id_prepare()?;
        let mut update_env_stmt = tx.update_env_prepare()?;
        let mut add_to_modify_env_stmt = tx.add_to_modify_prepare()?;
        for (env_var, env_val) in envs_to_insert {
            if let Ok((env_id, env_val_db)) = fetch_env_stmt.fetch_env_id(env_var.as_str()) {
                if !env_val_db.eq(&env_val) {
                    update_env_stmt.update_env_exec(env_id, env_val)?;
                    add_to_modify_env_stmt.add_to_modify_exec(env_id, Env)?;
                }
                crossref.add_env_xref(env_var, env_id);
                nodeids.insert((env_id, RowType::Env));
            } else {
                let env_id = inst_env_stmt.insert_env_exec(env_var.as_str(), env_val.as_str())?;
                crossref.add_env_xref(env_var, env_id);
                nodeids.insert((env_id, RowType::Env));
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
    Ok(nodeids)
}

pub struct NodeStatements<'a> {
    insert_node: SqlStatement<'a>,
    find_node: SqlStatement<'a>,
    update_mtime: SqlStatement<'a>,
    update_display_str: SqlStatement<'a>,
    update_flags: SqlStatement<'a>,
    update_srcid: SqlStatement<'a>,
}

impl NodeStatements<'_> {
    pub fn new(conn: &Connection) -> Result<NodeStatements> {
        let insert_node = conn.insert_node_prepare()?;
        let find_node = conn.fetch_node_prepare()?;
        let update_mtime = conn.update_mtime_prepare()?;
        let update_display_str = conn.update_display_str_prepare()?;
        let update_flags = conn.update_flags_prepare()?;
        let update_srcid = conn.update_srcid_prepare()?;
        Ok(NodeStatements {
            insert_node,
            find_node,
            update_mtime,
            update_display_str,
            update_flags,
            update_srcid,
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
}

pub(crate) struct AddIdsStatements<'a> {
    add_to_present: SqlStatement<'a>,
    add_to_modify: SqlStatement<'a>,
}

impl AddIdsStatements<'_> {
    pub fn new(conn: &Connection) -> Result<AddIdsStatements> {
        let add_to_present = conn.add_to_present_prepare()?;
        let add_to_modify = conn.add_to_modify_prepare()?;
        Ok(AddIdsStatements {
            add_to_present,
            add_to_modify,
        })
    }
    fn add_to_modify(&mut self, nodeid: i64, rowtype: RowType) -> crate::db::Result<()> {
        self.add_to_modify.add_to_modify_exec(nodeid, rowtype)
    }
    fn add_to_present(&mut self, nodeid: i64, rowtype: RowType) -> crate::db::Result<()> {
        self.add_to_present.add_to_present_exec(nodeid, rowtype)
    }
}

/// [find_upsert_node] pretends to be the sqlite upsert operation
/// it also adds the node to the present list, modify list and updates nodes mtime
pub(crate) fn find_upsert_node(
    node_statements: &mut NodeStatements,
    add_ids_statements: &mut AddIdsStatements,
    node: &Node,
) -> Result<Node> {
    debug!(
        "find_upsert_node:{} in dir:{}",
        node.get_name(),
        node.get_dir()
    );
    let db_node = node_statements
        .fetch_node(node.get_name(), node.get_dir())
        .and_then(|existing_node| {
            let mut modify = false;
            if (existing_node.get_mtime() - node.get_mtime()).abs() > 1 {
                debug!(
                    "updating mtime for:{}, {} -> {}",
                    existing_node.get_name(),
                    existing_node.get_mtime(),
                    node.get_mtime()
                );
                node_statements.update_mtime_exec(existing_node.get_id(), node.get_mtime())?;
                modify = true;
                add_ids_statements
                    .add_to_modify(existing_node.get_id(), *existing_node.get_type())?;
            }
            if existing_node.get_display_str() != node.get_display_str() {
                debug!(
                    "updating display_str for:{}, {} -> {}",
                    existing_node.get_name(),
                    existing_node.get_display_str(),
                    node.get_display_str()
                );
                node_statements
                    .update_display_str_exec(existing_node.get_id(), node.get_display_str())?;
                if !modify {
                    add_ids_statements
                        .add_to_modify(existing_node.get_id(), *existing_node.get_type())?;
                    modify = true;
                }
            }
            if existing_node.get_flags() != node.get_flags() {
                debug!(
                    "updating flags for:{}, {} -> {}",
                    existing_node.get_name(),
                    existing_node.get_flags(),
                    node.get_flags()
                );
                node_statements.update_flags_exec(existing_node.get_id(), node.get_flags())?;
                if !modify {
                    add_ids_statements
                        .add_to_modify(existing_node.get_id(), *existing_node.get_type())?;
                    modify = true;
                }
            }
            if existing_node.get_srcid() != node.get_srcid() {
                debug!(
                    "updating srcid for:{}, {} -> {}",
                    existing_node.get_name(),
                    existing_node.get_srcid(),
                    node.get_srcid()
                );
                node_statements.update_srcid_exec(existing_node.get_id(), node.get_srcid())?;
                if !modify {
                    add_ids_statements
                        .add_to_modify(existing_node.get_id(), *existing_node.get_type())?;
                    modify = true;
                }
            }
            if !modify {
                debug!(
                    "no change for:{}, {} -> {}",
                    existing_node.get_name(),
                    existing_node.get_mtime(),
                    node.get_mtime()
                );
            }
            add_ids_statements.add_to_present(existing_node.get_id(), *existing_node.get_type())?;
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

/// add links from targets that contribute to a group to the group id
fn add_links_to_groups(
    conn: &mut Connection,
    arts: &Artifacts,
    crossref: &CrossRefMaps,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut inp_linker = tx.insert_link_prepare()?;

        for (group_id, targets) in arts
            .get_resolved_links()
            .iter()
            .filter_map(|rl| rl.get_group_desc().as_ref().map(|g| (*g, rl.get_targets())))
        // filter those that have a group descriptor
        {
            let (group_db_id, _) = crossref
                .get_group_db_id(group_id)
                .ok_or_else(|| eyre!("group db id not found for group: {}", group_id))?;
            for target in targets {
                let (path_db_id, _) = crossref
                    .get_path_db_id(target)
                    .ok_or_else(|| eyre!("path db id not found for target: {}", target))?;
                inp_linker.insert_link(path_db_id, group_db_id, false, RowType::Grp)?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}

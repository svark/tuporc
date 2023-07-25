use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::ops::Deref;
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::Arc;
use std::thread;
use std::thread::yield_now;

use bimap::{BiHashMap, BiMap};
use crossbeam::channel::Receiver;
use crossbeam::sync::WaitGroup;
use eyre::{eyre, Report, Result};
use incremental_topo::IncrementalTopo;
use log::debug;
use num_cpus;
use parking_lot::Mutex;
use parking_lot::RwLock;
use rusqlite::Connection;

use tupetw::EventType::{ProcessCreation, ProcessDeletion, Write};
use tupetw::{DynDepTracker, EventHeader, EventType};
use tupparser::decode::{
    GlobPath, GlobPathDescriptor, MatchingPath, OutputHandler, OutputHolder, PathBuffers,
    PathSearcher, RuleRef,
};
use tupparser::errors::Error;
use tupparser::{
    Artifacts, GroupPathDescriptor, InputResolvedType, PathDescriptor, ReadWriteBufferObjects,
    ResolvedLink, RuleDescriptor, TupParser, TupPathDescriptor,
};

use crate::db::RowType::{Env, Excluded, GenF, Glob, Rule};
use crate::db::{
    create_dyn_io_temp_tables, create_path_buf_temptable, ForEachClauses, LibSqlExec,
    MiscStatements, SqlStatement,
};
use crate::{get_dir_id, LibSqlPrepare, Node, RowType, MAX_THRS_DIRS};

// CrossRefMaps maps paths, groups and rules discovered during parsing with those found in database
// These are two ways maps, so you can query both ways
#[derive(Debug, Clone, Default)]
pub struct CrossRefMaps {
    gbo: BiMap<GroupPathDescriptor, (i64, i64)>,
    pbo: BiMap<PathDescriptor, (i64, i64)>,
    rbo: BiMap<RuleDescriptor, (i64, i64)>,
    dbo: BiMap<TupPathDescriptor, (i64, i64)>,
    ebo: BiMap<String, i64>,
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
        ph: &mut (impl PathBuffers + Sized),
        glob_path: &GlobPath,
    ) -> Result<Vec<MatchingPath>> {
        let has_glob_pattern = glob_path.has_glob_pattern();
        if has_glob_pattern {
            debug!(
                "looking for matches in db for glob pattern: {:?}",
                glob_path.get_abs_path()
            );
        }
        let base_path = ph.get_path(glob_path.get_base_desc()).clone();
        let diff_path = ph.get_rel_path(&glob_path.get_path_desc(), glob_path.get_base_desc());
        let fetch_row = |s: &String| -> Option<MatchingPath> {
            debug!("found:{} at {:?}", s, base_path.as_path());
            let (pd, _) = ph.add_path_from(base_path.as_path(), Path::new(s.as_str()));
            if has_glob_pattern {
                let full_path = glob_path.get_base_abs_path().join(s.as_str());
                if glob_path.is_match(full_path.as_path()) {
                    let grps = glob_path.group(full_path.as_path());
                    Some(MatchingPath::with_captures(
                        pd,
                        glob_path.get_glob_desc(),
                        grps,
                    ))
                } else {
                    None
                }
            } else {
                Some(MatchingPath::new(pd))
            }
        };
        let conn = self.conn.deref().lock();
        let mut glob_query = conn.fetch_glob_nodes_prepare()?;
        let mps =
            glob_query.fetch_glob_nodes(base_path.as_path(), diff_path.as_path(), fetch_row)?;
        Ok(mps)
    }
}

impl PathSearcher for DbPathSearcher {
    fn discover_paths(
        &self,
        ph: &mut impl PathBuffers,
        glob_path: &GlobPath,
    ) -> std::result::Result<Vec<MatchingPath>, Error> {
        let mps = self
            .fetch_glob_nodes(ph, glob_path)
            .and_then(|mut x| {
                x.extend(self.get_outs().discover_paths(ph, glob_path)?);
                Ok(x)
            })
            .map_err(|e| Error::new_path_search_error(e.to_string().as_str(), RuleRef::default()));
        let c = |x: &MatchingPath, y: &MatchingPath| {
            let x = x.path_descriptor();
            let y = y.path_descriptor();
            x.cmp(y)
        };
        if let Ok(mut mps) = mps {
            mps.sort_by(c);
            mps.dedup();
            Ok(mps)
        } else {
            mps
        }
    }
    fn get_outs(&self) -> &OutputHolder {
        &self.psx
    }

    fn merge(&mut self, o: &impl OutputHandler) -> Result<(), Error> {
        OutputHandler::merge(&mut self.psx, o)
    }
}

/// handle the tup parse command which assumes files in db and adds rules and makes links joining input and output to/from rule statements
pub fn parse_tupfiles_in_db<P: AsRef<Path>>(
    tupfiles: Vec<Node>,
    root: P,
) -> Result<Vec<ResolvedLink>> {
    let mut crossref = CrossRefMaps::default();
    let (arts, mut rwbufs, mut outs) = {
        let conn = Connection::open(".tup/db")
            .expect("Connection to tup database in .tup/db could not be established");

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
                            //db.conn.
                            /*                            let deps = fetch_rule_deps.fetch_dependant_tupfiles(r.get_id())?;
                                                        tupfile_to_process
                                                            .extend(deps.into_iter().filter(|n| !visited.contains(&n)));
                            */
                            del_normal_link.delete_normal_rule_links(r.get_id())?;
                            del_outputs.mark_rule_outputs_deleted(r.get_id())?;
                        }
                    }
                }
            }
            tx.commit()?;
        }
        let tupfiles: Vec<_> = visited.into_iter().collect();
        let arts = gather_rules_from_tupfiles(&mut parser, tupfiles.as_slice())?;
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

    //XTODO: delete previous links from output files to groups
    add_links_to_groups(&mut conn, &arts, &crossref)?;
    fetch_group_provider_outputs(&mut conn, &mut rwbufs, &mut outs, &mut crossref)?;
    add_rule_links(&mut conn, &rwbufs, &arts, &mut crossref)?;
    // add links from glob inputs to tupfiles's directory
    add_link_glob_dir_to_rules(&mut conn, &rwbufs, &arts, &mut crossref)?;
    // mark rules as Modified if their inputs were deleted.
    // add or update rules if new glob inputs were discovered

    // now check if the outputs (deleted or modified) trigger any other rules
    // and if so, add them to the list of tupfiles to be processed

    //let rules = gather_rules_to_run(conn)?;

    Ok(Vec::new())
}

/// adds links from glob patterns specified at each directory that are inputs to rules  to the tupfile directory
/// We dont directly add links from glob patterns to rules, because already have resolved the glob patterns to paths in a previous iterations of parsing.
/// Newer / modified /deleted inputs discovered in glob patterns and added as rule inputs will be process in a  re-iteration parsing phase of Tupfile which the glob pattern links to
fn add_link_glob_dir_to_rules(
    conn: &mut Connection,
    rw_buf: &ReadWriteBufferObjects,
    arts: &Artifacts,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut insert_link = tx.insert_link_prepare()?;
        for rlink in arts.get_resolved_links() {
            let tupfile_desc = rlink.get_rule_ref().get_tupfile_desc();
            let (tupfile_db_id, _) = crossref.get_tup_db_id(tupfile_desc).unwrap_or_else(|| {
                panic!(
                    "tupfile dir not found:{:?} mentioned in rule {:?}",
                    tupfile_desc,
                    rlink.get_rule_ref()
                )
            });

            rlink.for_each_glob_path_desc(|glob_path_desc| -> Result<(), Error> {
                //               links_to_add.insert(glob_path_desc, tupfile_db_id);
                let (glob_pattern_id, _) =
                    crossref.get_glob_db_id(&glob_path_desc).unwrap_or_else(|| {
                        panic!(
                            "glob path not found:{:?} in tup db",
                            rw_buf.get_glob_path(&glob_path_desc)
                        )
                    });
                insert_link
                    .insert_link(glob_pattern_id, tupfile_db_id, true, RowType::TupF)
                    .map_err(|e| {
                        Error::new_path_search_error(
                            e.to_string().as_str(),
                            rlink.get_rule_ref().clone(),
                        )
                    })?;
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
    //create_tup_outputs(conn)?;

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
) -> Result<Artifacts> {
    //let mut del_stmt = conn.delete_tup_rule_links_prepare()?;
    let mut new_arts = Artifacts::new();
    let (sender, receiver) = crossbeam::channel::unbounded();
    let x = crossbeam::thread::scope(|s| -> Result<Artifacts> {
        let wg = WaitGroup::new();
        for ithread in 0..MAX_THRS_DIRS {
            for tupfile in tupfiles
                .iter()
                .filter(|x| !x.get_name().ends_with(".lua"))
                .cloned()
                .skip(ithread as usize)
                .step_by(MAX_THRS_DIRS as usize)
            {
                let mut p = p.clone();
                let sender = sender.clone();
                let wg = wg.clone();
                s.spawn(move |_| -> Result<()> {
                    p.send_tupfile(tupfile.get_name(), sender)
                        .map_err(|error| {
                            let rwbuf = p.read_write_buffers();
                            let display_str = rwbuf.display_str(&error);
                            Report::new(error).wrap_err(format!(
                                "Error while parsing tupfile: {}:\n {}",
                                tupfile.get_name(),
                                display_str
                            ))
                        })?;
                    drop(wg);
                    Ok(())
                });
            }
        }
        drop(sender);
        for tupfile_lua in tupfiles
            .iter()
            .filter(|x| x.get_name().ends_with(".lua"))
            .cloned()
        {
            let arts = p.parse(tupfile_lua.get_name())?;
            new_arts.extend(arts);
        }
        new_arts.extend(p.receive_resolved_statements(receiver).map_err(|error| {
            let read_write_buffers = p.read_write_buffers();
            let tup_node = read_write_buffers
                .get_tup_path(error.get_tup_descriptor())
                .to_string_lossy()
                .to_string();
            let display_str = read_write_buffers.display_str(error.get_error_ref());
            Report::new(error.get_error()).wrap_err(format!(
                "Unable to resolve statements in tupfile {}:\n{}",
                tup_node.as_str(),
                display_str
            ))
        })?);
        wg.wait();
        Ok(new_arts)
    });
    x.expect("Threading error while fetching artifacts from tupfiles")
}

pub(crate) fn exec_rules_to_run(
    mut conn: Connection,
    mut rule_nodes: Vec<Node>,
    fwd_refs: &BiMap<i32, incremental_topo::Node>,
    dag: &IncrementalTopo,
    root: &Path,
    target: &Vec<String>,
    keep_going: bool,
) -> Result<()> {
    // order the rules based on their dependencies
    let target_ids = conn.get_target_ids(root, target)?;
    let (trace_sender, trace_receiver) = crossbeam::channel::unbounded();
    let (completed_child_id_sender, completed_child_id_receiver) = crossbeam::channel::unbounded();
    let (spawned_child_id_sender, spawned_child_id_receiver) = crossbeam::channel::unbounded();
    let mut valid_rules = Vec::new();
    if target_ids.is_empty() {
        valid_rules = rule_nodes;
    } else {
        for r in rule_nodes.drain(..) {
            let id = r.get_id() as i32;
            if let Some(rule_node_in_dag) = fwd_refs.get_by_left(&id) {
                for target_id in target_ids.iter() {
                    let t = *target_id as i32;
                    if let Some(target_node_in_dag) = fwd_refs.get_by_left(&t) {
                        if dag.contains_transitive_dependency(rule_node_in_dag, target_node_in_dag)
                        {
                            valid_rules.push(r);
                            break;
                        }
                    }
                }
            }
        }
        if valid_rules.is_empty() {
            println!("No rules to run!");
            return Ok(());
        }
    }
    let mut topo_order = HashMap::new();
    let rule_ids = valid_rules
        .iter()
        .map(|r| r.get_id() as i32)
        .collect::<BTreeSet<i32>>();
    for r in valid_rules.iter() {
        let id = r.get_id() as i32;
        debug!("checking rule {} descendants in dag", id);
        fwd_refs
            .get_by_left(&id)
            .map(|rule_node_in_dag| -> Result<()> {
                let _ = topo_order.entry(id).or_insert(0);
                dag.descendants_unsorted(rule_node_in_dag)
                    .map(|x| {
                        x.for_each(|(order, ref dag_node)| {
                            let id = fwd_refs.get_by_right(dag_node).unwrap();
                            if rule_ids.contains(id) {
                                let o = topo_order.entry(*id).or_insert(order);
                                if order.cmp(o) == std::cmp::Ordering::Greater {
                                    *o = order;
                                }
                            }
                        });
                    })
                    .expect(&*format!(
                        "unable to sort rules to run from {}",
                        r.get_name()
                    ));
                Ok(())
            });
    }
    //let descendant_count = topo_order.len();
    let mut topo_orders_set = BTreeSet::new();
    for v in valid_rules.iter() {
        topo_orders_set.insert(topo_order.get(&(v.get_id() as i32)).unwrap());
    }
    //let mut childids = Vec::new();
    let poisoned = Arc::new(RwLock::new(0));
    let num_threads = std::cmp::min(num_cpus::get(), valid_rules.len());
    {
        let poisoned = poisoned.clone();
        ctrlc::set_handler(move || *poisoned.write() = 1).expect("Error setting Ctrl-C handler");
    }
    valid_rules.sort_by(|x, y| {
        let xid = x.get_id() as i32;
        let yid = y.get_id() as i32;
        topo_order
            .get(&xid)
            .unwrap()
            .cmp(topo_order.get(&yid).unwrap())
    });
    let mut dirpaths = Vec::new();
    {
        let mut dirpath = conn.fetch_node_path_prepare()?;
        for r in valid_rules.iter() {
            let path = dirpath.fetch_node_dir_path(r.get_dir())?;
            dirpaths.push(path);
        }
    }
    let mut min_idx = 0;
    /* let mut stream = termcolor::BufferedStandardStream::stdout(termcolor::ColorChoice::Always);
    stream.set_color(
        termcolor::ColorSpec::new()
            .set_fg(Some(termcolor::Color::Green))
            .set_bold(true),
    )
        .unwrap(); */

    let rule_for_child_id = Arc::new(RwLock::new(BTreeMap::new()));
    let mut tracker = tupetw::DynDepTracker::build(std::process::id(), trace_sender);
    tracker.start_and_process()?;
    crossbeam::scope(|s| -> Result<()> {
        {
            let poisoned = poisoned.clone();
            let trace_receiver = trace_receiver.clone();
            let child_id_receiver = completed_child_id_receiver.clone();
            let spawned_child_id_receiver = spawned_child_id_receiver.clone();
            s.spawn(move |_| -> Result<()> {
                if let Err(e) = listen_to_processes(
                    &mut conn,
                    root,
                    keep_going,
                    tracker,
                    poisoned.clone(),
                    &trace_receiver,
                    &child_id_receiver,
                    &spawned_child_id_receiver,
                ) {
                    eprintln!("Error while listening to processes: {}", e);
                    *poisoned.write() = 1;
                    return Err(e);
                }
                Ok(())
            });
        }
        drop(trace_receiver);
        drop(completed_child_id_receiver);
        for o in topo_orders_set {
            let max_idx = valid_rules.partition_point(|x| {
                let id = x.get_id() as i32;
                topo_order.get(&id).unwrap().cmp(&o) == std::cmp::Ordering::Less
            }); // we limit ourselves to nodes with same topo order, so that dependent rules are run later

            rule_for_child_id.write().clear();
            if min_idx == max_idx {
                continue;
            }
            let mut children = Vec::new();
            (0..num_threads).for_each(|_| {
                children.push(Vec::<Arc<Mutex<(Child, String)>>>::new());
            });
            for j in min_idx..max_idx {
                let rule_node = &valid_rules[j];
                if *poisoned.read() == 1 || (*poisoned.read() > 1 && !keep_going) {
                    let rule_id = rule_node.get_id();
                    let rule_name = rule_node.get_name();
                    completed_child_id_sender.send((0, (rule_id, rule_name.to_owned(), false)))?;
                    return Err(eyre!("Aborted executing rule: \n{}", rule_name));
                }
                let mut cmd = execute::shell(rule_node.get_name());
                cmd.current_dir(dirpaths[j].as_path());

                let ch = cmd.spawn()?;
                let ch_id = ch.id();
                if rule_node.get_display_str().is_empty() {
                    println!("id:{} {:?}", ch_id, cmd);
                } else {
                    println!("{} {}", ch_id, rule_node.get_display_str());
                }
                spawned_child_id_sender.send(ch_id)?;

                let rule_id = rule_node.get_id();
                rule_for_child_id
                    .write()
                    .insert(ch_id, (rule_id, rule_node.get_name().to_owned()));
                children[j % num_threads]
                    .push(Arc::new(Mutex::new((ch, rule_node.get_name().to_owned()))));
                //childids.push_back(ch_id);
                cmd.stdout(Stdio::piped());
                cmd.stderr(Stdio::piped());
            }

            {
                let wg = WaitGroup::new();
                let poisoned = poisoned.clone();
                for i in 0..num_threads {
                    let done = Arc::new(RwLock::new(false));
                    {
                        let poisoned = poisoned.clone();
                        let i = i.clone();
                        let children = children[i].clone();
                        // in this thread we wait for children to finish
                        let completed_child_id_sender = completed_child_id_sender.clone();
                        let rule_for_child_id = rule_for_child_id.clone();
                        let wg = wg.clone();
                        s.spawn(move |_| -> Result<()> {
                            let (finished, failed) =
                                wait_for_children(keep_going, poisoned, children)?;
                            for (id, succeeded) in finished
                                .iter()
                                .map(|i| (*i, true))
                                .chain(failed.iter().map(|j| (*j, false)))
                            {
                                let rule_for_child_id = rule_for_child_id.read();

                                if let Some((rule_id, rule_name)) =
                                    rule_for_child_id.get(&id).cloned()
                                {
                                    completed_child_id_sender
                                        .send((id, (rule_id, rule_name, succeeded)))?;
                                }
                            }
                            *done.write() = true;
                            drop(wg);
                            Ok(())
                        });
                    }
                }
                wg.wait(); // wait for all processes to finish before next topo order rules are executed
            }
            min_idx = max_idx;

            let p = poisoned.read();
            if *p != 0 {
                return Err(eyre!("Stopping further rule executions"));
            }
        } // min_topo_order..max topo order
        drop(completed_child_id_sender);
        drop(spawned_child_id_sender);
        Ok(())
    })
    .unwrap_or_else(|e| {
        eprintln!("Error while executing rules: {:?}", e);
        return Ok(());
    })
    .expect(" panic message");

    Ok(())
}

#[allow(dead_code)]
fn kill_poisoned(
    poisoned: &mut Arc<RwLock<u8>>,
    children: &Vec<Arc<Mutex<(Child, String)>>>,
    keep_going: bool,
) -> bool {
    let guard = poisoned.read();
    if *guard == 1 || *guard != 0 && !keep_going {
        children.into_iter().for_each(|ch| {
            let ref mut ch = ch.lock().0;
            let id = ch.id();
            ch.kill().ok();
            eprintln!("Killed child process {}", id);
        });
        true
    } else {
        false
    }
}

fn wait_for_children(
    _keep_going: bool,
    poisoned: Arc<RwLock<u8>>,
    mut children: Vec<Arc<Mutex<(Child, String)>>>,
) -> Result<(Vec<u32>, Vec<u32>), Report> {
    let (mut finished, mut failed) = (Vec::new(), Vec::new());
    while !children.is_empty() {
        let mut tryagain = Vec::new();

        for child in children.iter() {
            let ref mut ch = child.lock();
            let id = ch.0.id();
            if let Some(ref exit_status) = ch.0.try_wait()? {
                if !exit_status.success() {
                    if *poisoned.read() != 0 {
                        continue;
                    }
                    failed.push(id);
                    let mut poisoned = poisoned.write();
                    if *poisoned == 0 {
                        *poisoned = 2_u8;
                        yield_now();
                    }
                } else {
                    debug!("finished executing rule: \n{:?}", ch.1);
                    finished.push(id);
                }
            } else {
                tryagain.push(child.clone());
            }
        }
        children = tryagain;
        yield_now();
        if !children.is_empty() {
            std::thread::sleep(std::time::Duration::from_nanos(100));
            if *poisoned.read() != 0 {
                for child in children.iter() {
                    let ref mut ch = child.lock();
                    let id = ch.0.id();
                    failed.push(id);
                    ch.0.kill().ok();
                    ch.0.wait().ok();
                }
            }
        }
    }
    Ok((finished, failed))
}

fn listen_to_processes(
    conn: &mut Connection,
    root: &Path,
    keep_going: bool,
    mut tracker: DynDepTracker,
    poisoned: Arc<RwLock<u8>>,
    trace_receiver: &Receiver<EventHeader>,
    child_id_receiver: &Receiver<(u32, (i64, String, bool))>,
    spawned_child_id_receiver: &Receiver<u32>,
) -> Result<(), Report> {
    let mut io_conn =
        Connection::open(root.join(".tup/io.db")).expect("Failed to open in memory db");
    create_dyn_io_temp_tables(&mut io_conn)?;
    let mut end_completed_child_ids = false;
    let mut end_spawned_child_ids = false;
    let mut end_trace = false;
    let mut input_getter = conn.fetch_inputs_for_rule_prepare()?;
    let mut output_getter = conn.fetch_outputs_for_rule_prepare()?;
    let mut mark_failed_stmt = conn.mark_rule_failed_prepare()?;
    let mut fetch_id_stmt = conn.fetch_nodeid_prepare()?;
    let mut fetch_dirid_stmt = conn.fetch_dirid_prepare()?;
    let mut add_link_stmt = conn.insert_link_prepare()?;
    let mut deleted_child_procs = BTreeSet::new();
    let mut to_verify = Vec::new();
    let mut children = BTreeSet::new();
    loop {
        let mut sel = crossbeam::channel::Select::new();
        let index_child_ids = if end_completed_child_ids {
            usize::MAX
        } else {
            sel.recv(&child_id_receiver)
        };

        let index_trace = if end_trace {
            usize::MAX
        } else {
            sel.recv(&trace_receiver)
        };

        let index_spawned_child = if end_spawned_child_ids {
            usize::MAX
        } else {
            sel.recv(&spawned_child_id_receiver)
        };
        let mut change = false;
        while let Ok(oper) = sel.try_select() {
            if !keep_going && *poisoned.read() != 0 {
                break;
            }
            match oper.index() {
                i if i == index_child_ids => {
                    if let Ok((child_id, (rule_id, rule_name, succeeded))) =
                        oper.recv(&child_id_receiver).map_err(|_| {
                            log::debug!("no more children  expected");
                            end_completed_child_ids = true;
                        })
                    {
                        change = true;
                        if succeeded {
                            to_verify.push((child_id, rule_id, rule_name));
                        } else {
                            debug!("Error running rule: {}", rule_name);
                            mark_failed_stmt
                                .mark_rule_failed(rule_id as _)
                                .unwrap_or_else(|e| {
                                    panic!(
                                        "Could not write failed rule {} with id :{} to db, \n {}",
                                        rule_name,
                                        rule_id,
                                        e.to_string()
                                    )
                                });
                        }
                    }
                }
                i if i == index_trace => {
                    if let Ok(evt_header) = oper.recv(&trace_receiver).map_err(|_| {
                        log::debug!("no more trace events expected");
                        end_trace = true;
                    }) {
                        //dir_children_set.insert(p);
                        let file_path = evt_header.get_file_path();
                        let process_id = evt_header.get_process_id();
                        let process_gen = evt_header.get_process_gen();
                        let parent_process_id = evt_header.get_parent_process_id();
                        let event_type = evt_header.get_event_type();
                        change = true;
                        if event_type == Write as i8 {
                            debug!("Write recvd");
                        }
                        let child_cnt = evt_header.get_child_cnt() as i32;
                        if event_type == ProcessDeletion as _ || event_type == ProcessCreation as _
                        {
                            if event_type == ProcessDeletion as _ {
                                deleted_child_procs.insert(process_id);
                            }
                            io_conn
                                .insert_trace(
                                    file_path.as_str(),
                                    process_id as _,
                                    process_gen,
                                    event_type,
                                    child_cnt,
                                )
                                .expect("Failed to insert trace");
                        } else if let Some(rel_path) =
                            pathdiff::diff_paths(Path::new(file_path), root)
                        {
                            if !rel_path.starts_with("..") {
                                io_conn
                                    .insert_trace(
                                        rel_path.as_path(),
                                        parent_process_id as _,
                                        process_gen,
                                        event_type,
                                        child_cnt,
                                    )
                                    .expect("Failed to insert trace");
                            }
                        }
                    }
                }
                i if i == index_spawned_child => {
                    if let Ok(child_id) = oper.recv(&spawned_child_id_receiver).map_err(|_| {
                        log::debug!("no more spawned child ids expected");
                        end_spawned_child_ids = true;
                    }) {
                        debug!("spawned child id recvd :{}", child_id);
                        children.insert(child_id);
                        change = true;
                    }
                }
                _ => {
                    eprintln!("unknown index returned in select:{}", oper.index());
                    break;
                }
            }
            if end_spawned_child_ids && index_spawned_child != usize::MAX {
                break;
            }
            if end_trace && index_trace != usize::MAX {
                break;
            }
            if end_completed_child_ids && index_child_ids != usize::MAX {
                break;
            }
        }
        thread::sleep(std::time::Duration::from_nanos(100));
        if end_completed_child_ids && !end_trace {
            if index_child_ids != usize::MAX {
                // run select once more to pick up more eventheaders from trace_receiver
                yield_now();
                continue;
            }
            if end_spawned_child_ids {
                tracker.stop();
            }
        }
        if !to_verify.is_empty() {
            let mut fetch_io_stmt = io_conn
                .fetch_io_prepare()
                .expect("Failed to prepare fetch io statement");

            let mut reverify = Vec::new();
            for (child_id, rule_id, rule_name) in to_verify.drain(..) {
                if !deleted_child_procs.contains(&child_id) {
                    reverify.push((child_id, rule_id, rule_name));
                    continue;
                }
                deleted_child_procs.remove(&child_id);
                //dir_id_by_path.insert(p, id);
                if let Err(e) = verify_rule_io(
                    child_id,
                    rule_id as _,
                    rule_name.as_str(),
                    &mut input_getter,
                    &mut output_getter,
                    &mut fetch_io_stmt,
                    &mut add_link_stmt,
                    &mut fetch_id_stmt,
                    &mut fetch_dirid_stmt,
                ) {
                    eprintln!("Error verifying rule io: {}", e.to_string());
                    mark_failed_stmt
                        .mark_rule_failed(rule_id as _)
                        .unwrap_or_else(|e| {
                            panic!(
                                "Could not write failed rule {} with id :{} to db, \n {}",
                                rule_name,
                                rule_id,
                                e.to_string()
                            )
                        });
                    if *poisoned.read() == 0 {
                        *poisoned.write() = 2;
                    }
                }
            }
            to_verify = reverify;
        }
        if !keep_going && *poisoned.read() != 0 {
            break;
        }
        if end_completed_child_ids && end_trace && end_spawned_child_ids {
            break;
        }
        yield_now();
    }
    Ok(())
}

fn verify_rule_io(
    ch_id: u32,
    rule_id: i32,
    rule_name: &str,
    input_getter: &mut SqlStatement,
    output_getter: &mut SqlStatement,
    fetch_io_stmt: &mut SqlStatement,
    add_link_stmt: &mut SqlStatement,
    fetch_id_stmt: &mut SqlStatement,
    fetch_dirid_stmt: &mut SqlStatement,
) -> Result<()> {
    let io_vec = fetch_io_stmt.fetch_io(ch_id as i32)?;
    let inps = input_getter.fetch_inputs(rule_id)?;
    let outs = output_getter.fetch_outputs(rule_id)?;
    let mut processed_io = BTreeSet::new();
    'outer: for (fnode, ty) in io_vec.iter() {
        if !processed_io.insert((fnode.clone(), *ty)) {
            continue;
        }
        if *ty == EventType::Read as _ || *ty == EventType::Open as _ {
            for inp in inps.iter().chain(outs.iter()) {
                if *inp.get_type() == RowType::Dir
                    || *inp.get_type() == RowType::Grp
                    || *inp.get_type() == RowType::Env
                    || *inp.get_type() == RowType::DirGen
                {
                    continue;
                }
                if inp.get_name() == fnode {
                    continue 'outer;
                }
            }

            if let Ok(dirid) = fetch_dirid_stmt.fetch_dirid(fnode) {
                if let Ok(from_id) = fetch_id_stmt.fetch_node_id(fnode, dirid) {
                    add_link_stmt.insert_link(from_id, rule_id as _, false, RowType::Rule)?;
                }
            }
        } else if *ty == EventType::Write as u8 {
            for out in outs.iter() {
                if out.get_type().eq(&Excluded) {
                    let exclude_pattern = out.get_name().to_string();
                    use regex::Regex;
                    let re = Regex::new(&*exclude_pattern).unwrap();
                    if re.is_match(fnode) {
                        continue 'outer;
                    }
                }
                if out.get_name() == fnode {
                    continue 'outer;
                }
            }
            return Err(eyre!(
                "File {} being written was not an output to rule {}",
                fnode,
                rule_name
            ));
        }
    }
    'outer2: for inp in inps.iter() {
        let fname = inp.get_name();
        for (fnode, ty) in io_vec.iter() {
            if ty == &(EventType::Read as u8) || ty == &(EventType::Open as u8) && fnode == fname {
                continue 'outer2;
            }
        }
        eprintln!(
            "Proc:{} File {} was not read by rule {}",
            ch_id, fname, rule_name
        );
        //return Err(eyre!("File {} was not read by rule {}", fname, rule_name));
    }
    'outer3: for out in outs.iter() {
        let fname = out.get_name();
        for (fnode, ty) in io_vec.iter() {
            if ty == &(EventType::Write as u8) && fnode == fname {
                continue 'outer3;
            }
        }
        eprintln!(
            "Proc:{} File {} was not written by rule {}",
            ch_id, fname, rule_name
        );
        //return Err(eyre!("File {} was not written by rule {}",fname,rule_name));
    }
    Ok(())
}

pub(crate) fn prepare_for_execution(
    conn: &mut Connection,
) -> Result<(IncrementalTopo, BiHashMap<i32, incremental_topo::Node>)> {
    let mut dag = IncrementalTopo::new();
    let mut unique_node_ids = HashMap::new();
    {
        let add_edge = |x, y| -> Result<()> {
            let node1 = unique_node_ids
                .entry(x)
                .or_insert_with(|| dag.add_node())
                .clone();
            let node2 = unique_node_ids
                .entry(y)
                .or_insert_with(|| dag.add_node())
                .clone();
            let no_cyclic_dep = dag.add_dependency(node1, node2)?;
            if !no_cyclic_dep {
                Err(eyre!("Cyclic dependency detected!"))
            } else {
                Ok(())
            }
        };

        conn.for_each_link(add_edge)?
    }
    Ok((
        dag,
        BiHashMap::<i32, incremental_topo::Node>::from_iter(
            unique_node_ids.iter().map(|(x, y)| (*x, y.clone())),
        ),
    ))
}

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
                debug!(
                    "adding links from envs  {:?} to rule: {:?}",
                    env_desc, rule_node_id
                );
                for env_var in environs.get_keys() {
                    if let Some(env_id) = crossref.get_env_db_id(&env_var) {
                        inp_linker.insert_link(env_id, rule_node_id, false, RowType::Rule)?;
                    } else {
                        debug!("db env not found with descriptor {:?}", env_var);
                    }
                }
                log::debug!(
                    "adding links from inputs  {:?} to rule: {:?}",
                    rl.get_sources(),
                    rule_node_id
                );
                for i in rl.get_sources() {
                    let mut added: bool = false;
                    match i {
                        InputResolvedType::UnResolvedGroupEntry(g) => {
                            if let Some((group_id, _)) = crossref.get_group_db_id(&g) {
                                inp_linker.insert_link(
                                    group_id,
                                    rule_node_id,
                                    true,
                                    RowType::Rule,
                                )?;
                                added = true;
                            } else {
                                debug!("db group not found with descriptor {:?}", g);
                            }
                        }
                        InputResolvedType::Deglob(mp) => {
                            if let Some((pid, _)) = crossref.get_path_db_id(mp.path_descriptor()) {
                                debug!("slink {} => {}", pid, rule_node_id);
                                if processed.insert(pid) {
                                    inp_linker.insert_link(
                                        pid,
                                        rule_node_id,
                                        true,
                                        RowType::Rule,
                                    )?;
                                }
                                added = true;
                            } else {
                                debug!(
                                    "db path not found with descriptor {:?} => {:?}",
                                    mp.path_descriptor(),
                                    rbuf.get_input_path_str(&i)
                                );
                            }
                        }
                        InputResolvedType::BinEntry(_, p) => {
                            if let Some((pid, _)) = crossref.get_path_db_id(&p) {
                                debug!("bin slink {} => {}", pid, rule_node_id);
                                if processed.insert(pid) {
                                    inp_linker.insert_link(
                                        pid,
                                        rule_node_id,
                                        true,
                                        RowType::Rule,
                                    )?;
                                }
                                added = true;
                            } else {
                                debug!("bin entry not found:{:?}", p);
                            }
                        }
                        InputResolvedType::GroupEntry(g, p) => {
                            if let Some((group_id, _)) = crossref.get_group_db_id(&g) {
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
                                added = true;
                            } else {
                                debug!("group id not found in db:{}", g);
                            }
                        }
                        InputResolvedType::UnResolvedFile(f) => {
                            debug!("unresolved entry found during linking {}", f);
                        }
                    }
                    if !added {
                        let fname = rbuf.get_input_path_str(&i);

                        eyre::ensure!(
                            false,
                            format!(
                                "could not add a link from input {} to ruleid:{}",
                                fname, rule_node_id
                            )
                        );
                    }
                }
                {
                    log::debug!(
                        "adding links from rule  {:?} to outputs: {:?}",
                        rule_node_id,
                        rl.get_targets()
                    );

                    if let Some(g) = rl.get_group_desc() {
                        log::debug!("adding links from rule  {:?} to grp: {:?}", rule_node_id, g);
                        let (g, _) = crossref
                            .get_group_db_id(g)
                            .unwrap_or_else(|| panic!("failed to fetch db id of group {:?}", g));
                        out_linker.insert_link(rule_node_id, g, true, RowType::Grp)?;
                        for t in rl.get_targets() {
                            let (p, _) = crossref
                                .get_path_db_id(t)
                                .unwrap_or_else(|| panic!("failed to fetch db id of path {}", t));
                            out_linker.insert_link(p, g, true, RowType::Grp)?;
                            out_linker.insert_link(rule_node_id, p, true, RowType::GenF)?;
                        }
                    } else {
                        for t in rl.get_targets() {
                            let (p, _) = crossref
                                .get_path_db_id(t)
                                .unwrap_or_else(|| panic!("failed to fetch db id of path {}", t));
                            out_linker.insert_link(rule_node_id, p, true, RowType::GenF)?;
                        }
                    }
                    for t in rl.get_excluded_targets() {
                        let (p, _) = crossref
                            .get_path_db_id(t)
                            .unwrap_or_else(|| panic!("failed to fetch db id of path {}", t));
                        out_linker.insert_link(rule_node_id, p, true, RowType::Excluded)?;
                    }
                }
            }
        }
    }
    tconn.commit()?;
    Ok(())
}

// get a global list of outputfiles of a each group
fn fetch_group_provider_outputs(
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
        conn.for_each_grp_node_provider(groupid, Some(GenF), |node| -> Result<()> {
            // name of node is actually its path
            // merge providers of this group from all available in db
            let pd = rwbuf.add_abs(Path::new(node.get_name())).0;
            outs.add_group_entry(&group_desc, pd);
            Ok(())
        })?;
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
        .unwrap_or_else(|| panic!("No parent folder found for file {:?}", path));

    let (dir, parent_id) = fetch_dir_id.fetch_dirid_with_par(parent)?;
    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| panic!("missing name:{:?}", path));
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
            let parent = tupparser::transform::get_parent_str(group_path.as_path());
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
                    eyre::ensure!(
                        false,
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
                .unwrap_or_else(|| panic!("No parent folder found for file {:?}", path.as_path()));
            let dir_desc = read_write_buf
                .get_parent_id(p)
                .unwrap_or_else(|| panic!("descriptor not found for path:{:?}", parent));
            let (dir, par_dir) = {
                let x = find_dirid_with_par.fetch_dirid_with_par(parent);
                if x.is_err() {
                    debug!("failed to fetch dir id");
                }
                x?
            };
            crossref.add_path_xref(dir_desc, dir, par_dir);

            let name = path
                .as_path()
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| {
                    panic!("missing name:{:?} for a path to insert", path.as_path())
                });
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
        for r in arts.rules_by_tup().iter() {
            for rl in r.iter() {
                let rd = rl.get_rule_desc();
                let rule_ref = rl.get_rule_ref();
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
                            collect_nodes_to_insert(&p, &Glob, 0, dir, crossref, &mut find_nodeid)?;
                        }
                    }
                }
                for p in rl.get_excluded_targets() {
                    if processed.insert(p) {
                        collect_nodes_to_insert(p, &Excluded, 0, dir, crossref, &mut find_nodeid)?;
                    }
                }
                let env_desc = rl.get_env_desc();
                let environs = read_write_buf.get_envs(env_desc);
                envs_to_insert.extend(environs.getenv());
            }
        }
        for r in arts.rules_by_tup().iter() {
            debug!(
                "Cross referencing inputs to insert with the db ids with same name and directory"
            );
            for rl in r.iter() {
                for i in rl.get_sources() {
                    let inp = read_write_buf.get_input_path_str(i);
                    let p = i.get_resolved_path_desc();
                    if let Some(p) = p {
                        if processed.insert(p) {
                            if let Ok((nodeid, dirid, par_dir_id)) = find_by_path(
                                Path::new(inp.as_str()),
                                &mut find_nodeid,
                                &mut find_dir_id_with_parent,
                            ) {
                                let parent_id = read_write_buf
                                    .get_parent_id(p)
                                    .unwrap_or_else(|| panic!("no parent id found for:{:?}", p));
                                crossref.add_path_xref(*p, nodeid, dirid);
                                crossref.add_path_xref(parent_id, dirid, par_dir_id);
                            } else {
                                eyre::ensure!(
                                    false,
                                    "unresolved input found:{:?} for rule: {:?}",
                                    inp,
                                    rl.get_rule_ref()
                                );
                            }
                        }
                    } else {
                        eyre::ensure!(
                            false,
                            "unresolved input found:{:?} for rule: {:?}",
                            p,
                            rl.get_rule_ref()
                        );
                    }
                }
            }
        }
        nodeids.extend(existing_nodeids.iter());
    }
    let tx = conn.transaction()?;
    {
        //tx.create_temp_ids_table()?;
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
    fn insert_node_exec(&mut self, n: &Node) -> Result<i64> {
        self.insert_node.insert_node_exec(n)
    }
    fn fetch_node(&mut self, name: &str, dirid: i64) -> Result<Node> {
        self.find_node.fetch_node(name, dirid)
    }
    fn update_mtime_exec(&mut self, nodeid: i64, mtime: i64) -> Result<()> {
        self.update_mtime.update_mtime_exec(nodeid, mtime)
    }
    fn update_display_str_exec(&mut self, nodeid: i64, display_str: &str) -> Result<()> {
        self.update_display_str
            .update_display_str(nodeid, display_str)
    }
    fn update_flags_exec(&mut self, nodeid: i64, flags: &str) -> Result<()> {
        self.update_flags.update_flags_exec(nodeid, flags)
    }
    fn update_srcid_exec(&mut self, nodeid: i64, srcid: i64) -> Result<()> {
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
    fn add_to_modify(&mut self, nodeid: i64, rowtype: RowType) -> Result<()> {
        self.add_to_modify.add_to_modify_exec(nodeid, rowtype)
    }
    fn add_to_present(&mut self, nodeid: i64, rowtype: RowType) -> Result<()> {
        self.add_to_present.add_to_present_exec(nodeid, rowtype)
    }
}

// this is pretends to be the sqlite upsert operation
// it also adds the node to the present list, modify list and updates nodes mtime
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
            if let Some(rusqlite::Error::QueryReturnedNoRows) =
                e.root_cause().downcast_ref::<rusqlite::Error>()
            {
                let node = node_statements
                    .insert_node_exec(node)
                    .map(|i| Node::copy_from(i, node))?;
                add_ids_statements.add_to_modify(node.get_id(), *node.get_type())?;
                add_ids_statements.add_to_present(node.get_id(), *node.get_type())?; // add to present list
                Ok::<Node, eyre::Error>(node)
            } else {
                Err::<Node, eyre::Error>(e)
            }
        })?;
    Ok(db_node)
}

// add links from targets that are contribute to a group to the group itself
fn add_links_to_groups(
    conn: &mut Connection,
    arts: &Artifacts,
    crossref: &CrossRefMaps,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut inp_linker = tx.insert_link_prepare()?;

        for rl in arts.get_resolved_links() {
            if let Some(group_id) = rl.get_group_desc().as_ref() {
                if let Some((group_db_id, _)) = crossref.get_group_db_id(group_id) {
                    for target in rl.get_targets() {
                        if let Some((path_db_id, _)) = crossref.get_path_db_id(target) {
                            inp_linker.insert_link(path_db_id, group_db_id, false, RowType::Grp)?;
                        }
                    }
                }
            }
            /*for i in rl.get_sources() {
                if let InputResolvedType::GroupEntry(g, p) = i {
                    if let Some(group_id) = crossref.get_group_db_id(g) {
                        if let Some(pid) = crossref.get_path_db_id(p) {
                            inp_linker.insert_link(pid, GenF, group_id, Grp)?;
                        }
                    }
                }
            }*/
        }
    }
    tx.commit()?;
    Ok(())
}

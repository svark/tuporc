use crate::eyre::WrapErr;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};
use std::slice::Iter;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::thread::yield_now;

use bimap::hash::BiHashMap;
use bimap::BiMap;
use crossbeam::channel::{Receiver, Select, SelectedOperation};
//use ex::shell;
use eyre::{eyre, Report, Result};
use incremental_topo::IncrementalTopo;
use indicatif::ProgressBar;
use log::debug;
use parking_lot::{Mutex, RwLock};
use regex::{Regex, RegexBuilder};
use tupdb::db::{start_io_connection, Node, TupConnection, TupConnectionPool};
use tupetw::{DynDepTracker, EventHeader, EventType};
use tupparser::buffers::{BufferObjects, PathBuffers};
use tupparser::decode::decode_group_captures;
use tupparser::statements::{Loc, TupLoc};
use tupparser::TupPathDescriptor;
use incremental_topo::Node as N;

use crate::parse::{cancel_flag, ConnWrapper};
use crate::{TermProgress};
use tupdb::db::RowType;
use tupdb::db::RowType::Excluded;
use tupdb::error::{AnyError, DbResult};
use tupdb::inserts::LibSqlInserts;
use tupdb::queries::LibSqlQueries;

#[derive(Debug, Clone)]
struct PoisonedState {
    keep_going: bool,
    poisoned: Arc<AtomicBool>,
    errors: Arc<Mutex<Vec<Report>>>,
}

impl PoisonedState {
    fn new(keep_going: bool) -> Self {
        Self {
            keep_going,
            poisoned: Arc::new(AtomicBool::new(false)),
            errors: Arc::new(Mutex::new(Vec::new())),
        }
    }
    fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed) || cancel_flag().load(Ordering::Relaxed)
    }
    fn record_error(&self, err: Report) {
        let mut guard = self.errors.lock();
        log::error!("Error: {}", err);

        if self.keep_going || guard.is_empty() {
            guard.push(err);
        }
        if !self.keep_going {
            self.poisoned.store(true, Ordering::SeqCst);
        }
    }
    fn take_errors(&self) -> Option<Report> {
        let mut guard = self.errors.lock();
        if guard.is_empty() {
            return None;
        }
        if !self.keep_going {
            return Some(guard.remove(0));
        }
        let errs = std::mem::take(&mut *guard);
        let mut msg = String::from("Multiple errors occurred:\n");
        for (idx, err) in errs.iter().enumerate() {
            msg.push_str(&format!("{}. {}\n", idx + 1, err));
        }
        Some(eyre!(msg))
    }
    fn force_poisoned(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
    }
    fn should_stop(&self) -> bool {
        if cancel_flag().load(Ordering::Relaxed) {
            return true;
        }
        self.poisoned.load(Ordering::SeqCst)
    }
}
use std::collections::{HashSet, VecDeque};
/// Find one path `start -> ... -> goal` using only IncrementalTopo queries.
///
/// This is *debug-only* and can be expensive on big graphs because we don't have adjacency lists.
/// Strategy:
///  1) Candidate set = descendants(start) plus start
///  2) Prune candidates to only those that can still reach `goal` (via contains_transitive_dependency)
///  3) BFS where "neighbors(u)" are found by scanning `candidates` and testing contains_dependency(u,v)
fn find_path_via_queries(
    dag: &IncrementalTopo,
    start: &N,
    goal: &N,
    max_candidates: usize,
    max_bfs_visits: usize,
) -> Option<Vec<N>> {
    if start == goal {
        return Some(vec![start.clone()]);
    }

    // Quick sanity: If there's no transitive path, there's nothing to reconstruct.
    if !dag.contains_transitive_dependency(start, goal) {
        return None;
    }

    // 1) Collect descendants(start)
    let mut candidates: Vec<N> = Vec::new();
    candidates.push(start.clone());

    // descendants_unsorted yields (topo_rank, Node) pairs
    if let Ok(it) = dag.descendants_unsorted(start) {
        for (_rank, n) in it {
            candidates.push(n);
            if candidates.len() >= max_candidates {
                break; // safety cap
            }
        }
    }

    // 2) Prune: keep only nodes that can still reach goal (or are goal)
    //    This shrinks the scan space massively in many real graphs.
    let mut pruned: Vec<N> = Vec::with_capacity(candidates.len());
    for n in &candidates {
        if n == goal || dag.contains_transitive_dependency(n, goal) {
            pruned.push(n.clone());
        }
    }

    let pruned_set: HashSet<N> = pruned.iter().copied().collect();

    // 3) BFS with "neighbor discovery" by probing contains_dependency(u,v)
    let mut q = VecDeque::new();
    let mut parent: HashMap<N, N> = HashMap::new();

    q.push_back(start);
    parent.insert(*start, *start);

    let mut visits = 0usize;

    while let Some(u) = q.pop_front() {
        visits += 1;
        if visits > max_bfs_visits {
            break;
        }

        // neighbors(u) = { v in pruned_set | direct edge u->v exists }
        // contains_dependency checks direct edge existence
        for v in &pruned {
            if v == u || parent.contains_key(&v) {
                continue;
            }
            if !pruned_set.contains(&v) {
                continue;
            }
            if dag.contains_dependency(u, v) {
                parent.insert(*v, *u);

                if v == goal {
                    // reconstruct path
                    let mut path = vec![goal.clone()];
                    let mut cur = goal;
                    while cur != start {
                        cur = &parent[cur];
                        path.push(cur.clone());
                    }
                    path.reverse();
                    return Some(path);
                }

                q.push_back(v);
            }
        }
    }

    None
}

/// Helper: call this when add_dependency(prec, succ) fails with CycleDetected.
/// It prints the discovered cycle: succ -> ... -> prec -> succ
fn print_cycle_on_failure(
    dag: &IncrementalTopo,
    prec: N,
    succ: N,
    bimap: BiHashMap<i32, incremental_topo::Node>,
) -> String {
    // If add_dependency(prec, succ) fails with CycleDetected, then succ reaches prec already.
    // So we search succ -> ... -> prec.
    let path = find_path_via_queries(dag, &succ, &prec, 200_000, 5_000_000);

    let prec_db_id = bimap.get_by_right(&prec).cloned();
    let succ_db_id = bimap.get_by_right(&succ).cloned();
    match path {
        Some(p) => {
            let e1 = format!(" Adding edge prec -> succ would form a cycle:\n attempted edge: {:?} -> {:?}",
                             prec_db_id, succ_db_id);
            let e2 =
            format!(
                "  existing path:  {}",
                p.iter().map(|n| format!("{}", bimap.get_by_right(n).unwrap_or(&0))).collect::<Vec<_>>().join(" -> ")
            );
            let e3 = format!(
                "  full cycle:     {} -> {:?}",
                p.iter().map(|n| format!("{}", bimap.get_by_right(n).unwrap_or(&0))).collect::<Vec<_>>().join(" -> "),
                succ_db_id
            );
            e1 + &e2 + &e3
        }
        None => {
            "CycleDetected reported, but failed to reconstruct a path (caps too small?).".to_string()
        }
    }
}

fn prepare_for_execution(
    connection_pool: TupConnectionPool,
    term_progress: &TermProgress,
) -> Result<(IncrementalTopo, BiHashMap<i32, incremental_topo::Node>)> {
    let mut dag = IncrementalTopo::new();
    let mut unique_node_ids = HashMap::new();
    term_progress.clear();
    term_progress.set_message("Preparing for execution");
    let conn = connection_pool.get()?;
    let pbar = term_progress.make_progress_bar("Adding edges..");
    {
        let add_edge = |x, y| -> std::result::Result<(), AnyError> {
            let node1 = unique_node_ids
                .entry(x)
                .or_insert_with(|| dag.add_node())
                .clone();
            let node2 = unique_node_ids
                .entry(y)
                .or_insert_with(|| dag.add_node())
                .clone();
            dag.add_dependency(node1, node2).map_err(|_| {
                term_progress.abandon(&pbar, "Cyclic dependency detected!");
                let bimap = BiHashMap::<i32, incremental_topo::Node>::from_iter(
                    unique_node_ids.iter().map(|(x, n)| (*x, n.clone())),
                );
                let err = print_cycle_on_failure(&dag, node1, node2, bimap);
                AnyError::from(err)
            })?;
            term_progress.progress(&pbar);
            Ok(())
        };

        conn.for_each_link(add_edge)?
    }
    term_progress.finish(&pbar, "✔ Done adding edges");
    Ok((
        dag,
        BiHashMap::<i32, incremental_topo::Node>::from_iter(
            unique_node_ids.iter().map(|(x, y)| (*x, y.clone())),
        ),
    ))
}

#[derive(Debug, Clone)]
pub(crate) struct ExecOptions {
    pub keep_going: bool, // keep going even if some rules fail
    pub term_progress: TermProgress,
    pub num_jobs: usize, // number of parallel jobs
    pub timeout: std::time::Duration, // timeout for each rule
    /// Verbose output
    #[allow(unused)]
    pub verbose: bool,
}
impl Default for ExecOptions {
    fn default() -> Self {
        Self {
            keep_going: false,
            term_progress: TermProgress::new("Executing rules"),
            num_jobs: num_cpus::get(),
            verbose: false,
            timeout: std::time::Duration::from_secs(15 * 60), // timeout for each rule
        }
    }
}

pub(crate) fn execute_targets(
    connection_pool: &TupConnectionPool,
    target: &Vec<String>,
    root: PathBuf,
    exec_options: &ExecOptions,
) -> Result<()> {

    let (dag, node_bimap) =
        prepare_for_execution(connection_pool.clone(), &exec_options.term_progress)?;

    //create_dyn_io_temp_tables(&conn)?;
    // start tracking file io by subprocesses.
    let tup_connection = connection_pool.get()?;
    let tup_connection_ref = tup_connection.as_ref();
    let conn_wrapper = ConnWrapper::new(&tup_connection_ref);
    let f = |node: Node| -> DbResult<Node> {
        let rule_id = node.get_id();
        let rule_ref = TupLoc::new(
            &TupPathDescriptor::default(),
            &Loc::new(node.get_srcid() as _, 0, 0),
        );
        let rule_string = node.get_name();
        let new_name = decode_group_captures(
            &conn_wrapper,
            &rule_ref,
            rule_id,
            node.get_dir(),
            rule_string,
        );
        if matches!(new_name, Cow::Borrowed(_)) {
            return Ok(node); // no replacements done
        }

        Ok(Node::new_rule(
            node.get_id(),
            node.get_dir(),
            new_name.to_string(),
            node.get_display_str().to_string(),
            node.get_flags().to_string(),
            node.get_srcid() as _,
        ))
    };
    let mut rule_nodes = tup_connection.fetch_rules_to_run()?;
    rule_nodes.extend(tup_connection.fetch_tasks_to_run()?);
    let rule_nodes = rule_nodes
        .into_iter()
        .map(f)
        .collect::<DbResult<Vec<Node>>>()?;
    if rule_nodes.is_empty() {
        println!("Nothing to do");
        return Ok(());
    }
    drop(tup_connection);
    let _ = exec_nodes_to_run(
        connection_pool.clone(),
        rule_nodes,
        &node_bimap,
        &dag,
        root.as_path(),
        &target,
        &exec_options,
    )?;

    Ok(())
}

#[derive(Debug, Clone)]
struct ProcReceivers {
    child_id_receiver: Receiver<(u32, (i64, String, bool))>,
    trace_receiver: Receiver<EventHeader>,
    spawned_child_id_receiver: Receiver<u32>,
    end_completed_child_ids: bool,
    end_spawned_child_ids: bool,
    end_trace: bool,
}

impl ProcReceivers {
    fn new(
        child_id_receiver: Receiver<(u32, (i64, String, bool))>,
        trace_receiver: Receiver<EventHeader>,
        spawned_child_id_receiver: Receiver<u32>,
    ) -> Self {
        Self {
            child_id_receiver,
            trace_receiver,
            spawned_child_id_receiver,
            end_completed_child_ids: false,
            end_spawned_child_ids: false,
            end_trace: false,
        }
    }
    fn is_empty(&self) -> bool {
        self.end_completed_child_ids && self.end_spawned_child_ids && self.end_trace
    }
    fn should_stop_trace(&self) -> bool {
        self.end_completed_child_ids && self.end_spawned_child_ids && !self.end_trace
    }

    fn get_index<'a, P: 'a>(
        receiver: &'a Receiver<P>,
        condition: bool,
        select: &mut Select<'a>,
    ) -> usize {
        if condition {
            usize::MAX
        } else {
            select.recv(receiver)
        }
    }

    fn handle_child_ids<'a>(
        &'a self,
        operation: SelectedOperation<'a>,
        process_checker: &mut ProcessIOChecker,
        rules_to_verify: &mut RulesToVerify,
    ) -> Result<bool> {
        let mut end_receive = false;
        if let Ok((child_id, (rule_id, rule_name, succeeded))) = {
            operation.recv(&self.child_id_receiver).map_err(|_| {
                debug!("no more children  expected");
                end_receive = true;
            })
        } {
            log::info!(
                "Child id {} finished executing rule: {}",
                child_id,
                rule_name
            );
            if succeeded {
                rules_to_verify.add(child_id, rule_id, rule_name);
            } else {
                debug!("Error running rule: {}", rule_name);
                process_checker
                    .mark_rule_succeeded(rule_id as _)
                    .map_err(|e| {
                        eyre!(
                            "Could not write failed rule {} with id :{} to db, \n {}",
                            rule_name,
                            rule_id,
                            e.to_string()
                        )
                    })?;
            }
        }
        Ok(end_receive)
    }

    fn handle_trace(
        &self,
        operation: SelectedOperation,
        root: &Path,
        deleted_child_procs: &mut std::collections::BTreeSet<u32>,
        io_conn: &mut IoConn,
    ) -> Result<bool> {
        let r = &self.trace_receiver;
        let mut end = false;
        if let Ok(evt_header) = operation.recv(r).map_err(|_| {
            debug!("no more trace events expected");
            end = true;
        }) {
            let process_id = evt_header.get_process_id();
            let event_type = evt_header.get_event_type();
            if event_type == EventType::ProcessDeletion as _ {
                deleted_child_procs.insert(process_id);
            }
            io_conn
                .insert_trace(root, &evt_header)
                .expect("Failed to insert trace");
        }
        Ok(end)
    }

    fn handle_spawned_child(
        &self,
        operation: SelectedOperation,
        children: &mut std::collections::BTreeSet<u32>,
    ) -> bool {
        let r = &self.spawned_child_id_receiver;
        let mut end = false;
        if let Ok(child_id) = operation.recv(r).map_err(|_| {
            debug!("no more spawned children expected");
            end = true;
        }) {
            children.insert(child_id);
        }
        end
    }

    fn should_break(
        &self,
        index_spawned_child: usize,
        index_trace: usize,
        index_child_ids: usize,
    ) -> bool {
        if self.end_spawned_child_ids && index_spawned_child != usize::MAX {
            return true;
        }
        if self.end_trace && index_trace != usize::MAX {
            return true;
        }
        if self.end_completed_child_ids && index_child_ids != usize::MAX {
            return true;
        }
        false
    }
    fn process_sel<'a>(
        &'a mut self,
        process_checker: &mut ProcessIOChecker,
        rules_to_verify: &mut RulesToVerify,
        root: &Path,
        deleted_child_procs: &mut std::collections::BTreeSet<u32>,
        io_conn: &mut IoConn,
        children: &mut std::collections::BTreeSet<u32>,
        poisoned: &PoisonedState,
    ) -> Result<(usize, usize, usize)> {
        {
            let ref mut sel = Select::new();
            let index_child_ids =
                Self::get_index(&self.child_id_receiver, self.end_completed_child_ids, sel);
            let index_trace = Self::get_index(&self.trace_receiver, self.end_trace, sel);
            let index_spawned_child = Self::get_index(
                &self.spawned_child_id_receiver,
                self.end_spawned_child_ids,
                sel,
            );

            while let Ok(operation) = sel.try_select() {
                match operation.index() {
                    i if i == index_child_ids => {
                        self.end_completed_child_ids =
                            self.handle_child_ids(operation, process_checker, rules_to_verify)?;
                    }
                    i if i == index_trace => {
                        self.end_trace =
                            self.handle_trace(operation, root, deleted_child_procs, io_conn)?;
                    }
                    i if i == index_spawned_child => {
                        self.end_spawned_child_ids = self.handle_spawned_child(operation, children);
                    }
                    _ => {
                        unreachable!("unexpected index");
                    }
                }
                if self.should_break(index_spawned_child, index_trace, index_child_ids) {
                    break;
                }
                if poisoned.is_poisoned() {
                    break;
                }
            }
            Ok((index_child_ids, index_trace, index_spawned_child))
        }
    }
}

fn get_target_ids(conn: &TupConnection, root: &Path, targets: &Vec<String>) -> Result<Vec<i64>> {
    let dir = std::env::current_dir()?;
    let bo = BufferObjects::new(root);
    let dir_desc = bo
        .add_abs(&dir)
        .wrap_err("While adding current directory to path list")?;
    let mut dir_ids = Vec::new();
    for t in targets {
        let path_desc = dir_desc.join(t.as_str())?;
        let path = bo.get_path(&path_desc);
        let id = conn.fetch_dirid_by_path(path.as_path())
            .wrap_err_with(|| format!("Target not found: {}", t))?;
        dir_ids.push(id);
    }
    let mut rule_ids = Vec::new();
    for dirid in dir_ids.iter() {
        conn.for_each_rule_in_dir(*dirid, |r| {
            rule_ids.push(r);
            Ok(())
        })?;
    }
    Ok(rule_ids)
}
fn execute_rule(
    rule_node: &Node,
    dirpath: &Path,
    spawned_child_id_sender: &crossbeam::channel::Sender<u32>,
    children: &mut Vec<Arc<Mutex<(Child, String, Instant)>>>,
    term_progress: &TermProgress,
) -> Result<()> {
    let  command_line = rule_node.get_name().to_owned();
    let mut cmd = tupcompat::shell(&command_line);
    cmd.current_dir(dirpath);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut ch = cmd.spawn()?;
    let ch_id = ch.id();
    if command_line.len() > 8000 {
        if let Some(mut stdin) = ch.stdin.take() {
            use std::io::Write;
            stdin.write_all(command_line.as_bytes())?;
        }
    }
    if rule_node.get_display_str().is_empty() {
        let regex = RegexBuilder::new(r#"cl.*\s+(\w)$"#);
        let re = regex.build().unwrap();
        if re.is_match(command_line.as_str()) {
            if let Some(c) = re.captures(command_line.as_str()) {
                if let Some(Some(fname)) = c.iter().next() {
                    let msg = format!("Cl {}", fname.as_str());
                    term_progress.set_main_message(msg);
                }
            }
        }
        //term_progress.println(format!("{}", command_line));

    } else {
        let msg = format!("{}",  rule_node.get_display_str());
        term_progress.set_main_message(msg);
    }
    spawned_child_id_sender.send(ch_id)?;

    children.push(Arc::new(Mutex::new((ch, rule_node.get_name().to_owned(), Instant::now()))));
    Ok(())
}

fn exec_nodes_to_run(
    connection_pool: TupConnectionPool,
    mut rule_nodes: Vec<Node>,
    fwd_refs: &BiMap<i32, incremental_topo::Node>,
    dag: &IncrementalTopo,
    root: &Path,
    target: &Vec<String>,
    exec_options: &ExecOptions,
) -> Result<()> {
    // order the rules based on their dependencies
    let keep_going = exec_options.keep_going;
    let num_threads = exec_options.num_jobs;
    let term_progress = &exec_options.term_progress;
    let timeout =  &exec_options.timeout;
    let conn = connection_pool.get()?;
    let target_ids = get_target_ids(&conn, root, target)?;
    if !target.is_empty() && target_ids.is_empty() {
        return Err(eyre!("No rules found for targets: {:?}", target));
    }
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
                    if t == id {
                        valid_rules.push(r);
                        break;
                    }
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
        .collect::<std::collections::BTreeSet<i32>>();
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
    let mut topo_orders_set = std::collections::BTreeSet::new();
    for v in valid_rules.iter() {
        topo_orders_set.insert(topo_order.get(&(v.get_id() as i32)).unwrap());
    }
    let poisoned = PoisonedState::new(keep_going);
    let num_threads = std::cmp::min(connection_pool.max_size() as usize,
                                    std::cmp::min(num_threads, valid_rules.len()));
    term_progress.set_main_len(valid_rules.len() as u64, "Executing rules");
    let _ = cancel_flag();
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
        for r in valid_rules.iter() {
            let path = conn.fetch_dirpath(r.get_dir()).map_err(|_| {
                AnyError::from(format!(
                    "unable to fetch dirpath {} for rule: {}",
                    r.get_dir(),
                    r.get_name()
                ))
            })?;
            dirpaths.push(path);
        }
    }
    let mut min_idx = 0;

    let rule_for_child_id: BTreeMap<u32, (i64, String)> = BTreeMap::new();
    let rule_for_child_id = Arc::from(RwLock::new(rule_for_child_id));
    let mut tracker = DynDepTracker::build(std::process::id(), trace_sender);
    tracker.start_and_process()?;
    let thread_pool = rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads + 1)
        .build()
        .expect("Failed to build thread pool to execute rules");
    let res = thread_pool.scope(|s| -> Result<()> {
        {
            let poisoned = poisoned.clone();
            let proc_receivers = ProcReceivers::new(
                completed_child_id_receiver,
                trace_receiver,
                spawned_child_id_receiver,
            );
            let pool = connection_pool.clone();
            s.spawn(move |_| {
                let mut conn = pool.get().expect("Failed to get connection from pool");
                if let Err(e) =
                    listen_to_processes(&mut conn, root, tracker, poisoned.clone(), proc_receivers)
                {
                    eprintln!("Error while listening to processes: {}", e);
                    poisoned.force_poisoned();
                }
            });
        }
        for o in topo_orders_set {
            let max_idx = valid_rules.partition_point(|x| {
                let id = x.get_id() as i32;
                topo_order.get(&id).unwrap().cmp(&o) != std::cmp::Ordering::Greater
            }); // we limit ourselves to nodes with same topo order, so that dependent rules are run later

            rule_for_child_id.write().clear();
            if min_idx == max_idx {
                continue;
            }
            let (child_to_wait_tx, child_to_wait_rx) = crossbeam::channel::unbounded::<Arc<Mutex<(Child, String, Instant)>>>();
            let (slot_tx, slot_rx) = crossbeam::channel::bounded(num_threads);
            for _ in 0..num_threads {
                slot_tx.send(()).unwrap(); // initially we allow execution of num_threads rules,
                // after that a thread will only send on slot_tx after it finishes executing a rule and is ready to execute another one
            }

            let wg = crossbeam::sync::WaitGroup::new();
            let layer_pbar = term_progress.make_child_len_progress_bar("Current Layer", (max_idx - min_idx) as u64);
            for _ in 0..num_threads {
                let poisoned = poisoned.clone();
                let child_to_wait_rx = child_to_wait_rx.clone();
                let completed_child_id_sender = completed_child_id_sender.clone();
                let rule_for_child_id = rule_for_child_id.clone();
                let wg = wg.clone();
                let pbar = layer_pbar.clone();
                let term_progress = term_progress.clone();
                let slot_tx = slot_tx.clone();
                s.spawn(move |_| {
                    while let Ok(child) = child_to_wait_rx.recv() {
                        let (finished, failed) =
                            wait_for_children(poisoned.clone(), vec![child], &pbar, &term_progress, &timeout);
                        for (id, succeeded) in finished
                            .iter()
                            .map(|i| (*i, true))
                            .chain(failed.iter().map(|j| (*j, false)))
                        {
                            let rule_for_child_id = rule_for_child_id.read();
                            if let Some((rule_id, rule_name)) = rule_for_child_id.get(&id).cloned() {
                                completed_child_id_sender
                                    .send((id, (rule_id, rule_name, succeeded)))
                                    .expect("Failed to send completed child id");
                            }
                        }
                        slot_tx.send(()).ok();
                    }
                    drop(wg);
                });
            }
            drop(child_to_wait_rx);

            for (j, _) in (min_idx..max_idx).zip(&slot_rx) {
                let rule_node = &valid_rules[j];
                if poisoned.should_stop() {
                    let rule_id = rule_node.get_id();
                    let rule_name = rule_node.get_name();
                    completed_child_id_sender.send((0, (rule_id, rule_name.to_owned(), false)))?;
                    term_progress.abandon_main("Aborted");
                    eyre::bail!("Aborted executing rule: \n{}", rule_name);
                } else {
                    let mut children = Vec::new();
                    execute_rule(
                        rule_node,
                        &dirpaths[j],
                        &spawned_child_id_sender,
                        &mut children,
                        term_progress,
                    )?;
                    for child in children {
                        let id = child.lock().0.id();
                        rule_for_child_id.write().insert(id, (rule_node.get_id(), rule_node.get_name().to_owned()));
                        child_to_wait_tx.send(child).unwrap();
                    }
                }
            }
            drop(child_to_wait_tx);

            wg.wait(); // wait for all processes to finish before next topo order rules are executed
            layer_pbar.finish_and_clear();
            min_idx = max_idx;

            if poisoned.should_stop() {
                term_progress.abandon_main("Stopping further rule executions");
                eyre::bail!("Stopping further rule executions");
            }
        } // min_topo_order..max topo order
        drop(completed_child_id_sender);
        drop(spawned_child_id_sender);
        if let Some(err) = poisoned.take_errors() {
            return Err(err);
        }
        Ok(())
    });
    res?;
    Ok(())
}

fn kill_poisoned(children: &Vec<Arc<Mutex<(Child, String, Instant)>>>) -> Vec<u32> {
    let mut failed_children = Vec::new();
    children.into_iter().for_each(|ch| {
        let ref mut ch = ch.lock().0;
        let id = ch.id();
        failed_children.push(id);
        ch.kill().ok();
        //eprintln!("Killed child process {}", id);
    });
    failed_children
}

fn wait_for_children(
    poisoned: PoisonedState,
    mut children: Vec<Arc<Mutex<(Child, String, Instant)>>>,
    pbar: &ProgressBar,
    term_progress: &TermProgress,
    timeout : &Duration
) -> (Vec<u32>, Vec<u32>) {
    let (mut finished, mut failed) = (Vec::new(), Vec::new());
    while !children.is_empty() {
        let mut tryagain = Vec::new();

        for child in children.iter() {
            let ref mut ch = child.lock();
            let id = ch.0.id();
            if let Ok(Some(ref exit_status)) = ch.0.try_wait() {
                if !exit_status.success() {
                    failed.push(id);
                    let err = eyre!("Rule failed: {}", ch.1);
                    poisoned.record_error(err);
                    yield_now();
                } else {
                    debug!("finished executing rule: \n{:?}", ch.1);
                    finished.push(id);
                }
                term_progress.progress(pbar);
            } else {
                if ch.2.elapsed() > *timeout {
                    failed.push(id);
                    let err = eyre!("Rule timed out (15 mins): {}", ch.1);
                    poisoned.record_error(err);
                    ch.0.kill().ok();
                    eprintln!("Killed child process {} due to timeout", id);
                    term_progress.progress(pbar);
                } else {
                    tryagain.push(child.clone());
                }
            }
        }
        children = tryagain;
        yield_now();
        if !children.is_empty() {
            thread::sleep(std::time::Duration::from_millis(1));
            // kill the remaining children
            if poisoned.should_stop() {
                failed.extend(kill_poisoned(&children).drain(..));
                break;
            }
        }
    }
    (finished, failed)
}

struct ProcessIOChecker<'a> {
    conn: &'a TupConnection,
}

struct IoConn<'b> {
    conn: &'b TupConnection,
}

struct RulesToVerify {
    to_verify: Vec<(u32, i64, String)>,
}

impl RulesToVerify {
    fn is_empty(&self) -> bool {
        self.to_verify.is_empty()
    }

    fn iter(&self) -> Iter<'_, (u32, i64, String)> {
        self.to_verify.iter()
    }

    fn verify_again(&mut self, to_verify: Vec<(u32, i64, String)>) {
        self.to_verify = to_verify;
    }

    fn add(&mut self, child_id: u32, rule_id: i64, rule_name: String) {
        self.to_verify.push((child_id, rule_id, rule_name));
    }
}

impl<'a> ProcessIOChecker<'a> {
    fn new(conn: &'a mut TupConnection) -> Result<Self> {
        let s = Self { conn };
        Ok(s)
    }

    fn fetch_node_by_id(&self, node_id: i64) -> Result<Node> {
        let node = self
            .conn
            .fetch_node_by_id(node_id)?
            .ok_or_else(|| eyre!("Node with id {} not found", node_id))?;
        Ok(node)
    }
    fn fetch_inputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        let mut nodes = Vec::new();
        self.conn.for_each_rule_input(rule_id as _, |node| {
            nodes.push(node);
            Ok(())
        })?;
        Ok(nodes)
    }
    fn fetch_srcid(&mut self, rule_id: i32) -> Result<i64> {
        let srcid = self.conn.fetch_node_srcid(rule_id as _)?;
        Ok(srcid)
    }

    fn fetch_outputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        //let fetch_outputs = self.fetch_rule_outputs(rule_id)?;
        let mut nodes = Vec::new();
        self.conn.for_each_rule_output(rule_id as _, |node| {
            nodes.push(node);
            Ok(())
        })?;
        Ok(nodes)
    }

    fn fetch_flags(&mut self, rule_id: i32) -> Result<String> {
        let flags = self.conn.fetch_flags(rule_id as _)?;
        Ok(flags)
    }

    fn fetch_dirid<P: AsRef<Path>>(&mut self, node_path: P) -> Result<i64> {
        let dirid = self.conn.fetch_dirid_by_path(node_path)?;
        Ok(dirid)
    }
    fn fetch_node_id(&mut self, node_name: &str, dirid: i64) -> Result<i64> {
        let nodeid = self.conn.fetch_node_id_by_dir_and_name(dirid, node_name)?;
        Ok(nodeid)
    }

    fn mark_rule_succeeded(&mut self, rule_id: i64) -> Result<()> {
        self.conn.mark_rule_succeeded(rule_id)?;
        Ok(())
    }

    fn insert_link(&mut self, from_id: i64, to_id: i64, tup_id: i64, is_output: bool) -> Result<()> {
        self.conn
            .insert_link(from_id, to_id, is_output.into(), tup_id, RowType::Rule)?;
        Ok(())
    }

}

impl<'b> IoConn<'b> {
    fn new(conn: &'b mut TupConnection) -> Result<Self> {
        let s = Self { conn };
        Ok(s)
    }

    fn fetch_io(&mut self, child_id: u32) -> Result<Vec<(String, u8)>> {
        let fetch_io = self.conn.fetch_io(child_id as _)?;
        Ok(fetch_io)
    }

    fn insert_trace(&mut self, root: &Path, evt_header: &EventHeader) -> Result<()> {
        let file_path = evt_header.get_file_path();
        let process_id = evt_header.get_process_id();
        let process_gen = evt_header.get_process_gen();
        let parent_process_id = evt_header.get_parent_process_id();
        let event_type = evt_header.get_event_type();
        let child_cnt = evt_header.get_child_cnt() as i32;
        match event_type {
            EventType::ProcessCreation | EventType::ProcessDeletion => {
                if process_id == 0 || file_path.as_str().trim().is_empty() {
                    return Ok(());
                }
                self.conn.insert_trace(
                    file_path.as_str(),
                    process_id as _,
                    process_gen as _,
                    event_type as u8,
                    child_cnt as _,
                )?;
            }

            EventType::Open | EventType::Read | EventType::Write => {
                // only add paths relative to root
                if let Some(rel_path) = pathdiff::diff_paths(Path::new(file_path.as_str()), root) {
                    if !rel_path.starts_with("..") {
                        if event_type == EventType::Write {
                            debug!("Write recvd for {} by process id:{}", file_path, process_id);
                        }
                        if event_type == EventType::Read || event_type == EventType::Open {
                            debug!(
                                "Open/Read recvd for {} by process id:{}",
                                file_path, process_id
                            );
                        }

                        self.conn.insert_trace(
                            &rel_path.as_path().to_string_lossy().to_string(),
                            parent_process_id as _,
                            process_gen as _,
                            event_type as u8,
                            child_cnt as _,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
}

// Then use this function like this:

fn listen_to_processes(
    conn: &mut TupConnection,
    root: &Path,
    mut tracker: DynDepTracker,
    poisoned: PoisonedState,
    mut proc_receivers: ProcReceivers,
) -> Result<()> {
    let io_conn_pool = start_io_connection( 2 ).expect("Failed to open in memory db");
    let mut io_conn = io_conn_pool.get()?;
    let mut process_checker = ProcessIOChecker::new(conn)?;
    let mut deleted_child_procs = std::collections::BTreeSet::new();

    let mut rules_to_verify = RulesToVerify {
        to_verify: Vec::new(),
    };
    let mut io_conn = IoConn::new(&mut io_conn)?;
    let mut children = std::collections::BTreeSet::new();
    loop {
        proc_receivers.process_sel(
            &mut process_checker,
            &mut rules_to_verify,
            root,
            &mut deleted_child_procs,
            &mut io_conn,
            &mut children,
            &poisoned,
        )?;
        thread::sleep(std::time::Duration::from_nanos(100));
        if poisoned.should_stop() || proc_receivers.should_stop_trace() {
            tracker.stop();
        }
        if !rules_to_verify.is_empty() {
            let mut re_verify = Vec::new();
            for (child_id, rule_id, rule_name) in rules_to_verify.iter() {
                if !deleted_child_procs.contains(&child_id) {
                    re_verify.push((*child_id, *rule_id, rule_name.clone()));
                    continue;
                }
                deleted_child_procs.remove(&child_id);
                let node = process_checker.fetch_node_by_id(*rule_id)?;
                if let Err(e) = verify_node_io(*child_id, &node, &mut io_conn, &mut process_checker)
                {
                    let err = eyre!("Error verifying rule io {}\n{}", rule_name, e.to_string());
                    poisoned.record_error(err);
                } else {
                    process_checker
                        .mark_rule_succeeded(*rule_id as _)
                        .unwrap_or_else(|e| {
                            panic!(
                                "Could not write success of rule {} with id :{} to db, \n {}",
                                rule_name,
                                rule_id,
                                e.to_string()
                            )
                        });
                }
            }
            rules_to_verify.verify_again(re_verify);
        }
        if poisoned.should_stop() {
            break;
        }
        if proc_receivers.is_empty() {
            break;
        }
        yield_now();
    }
    Ok(())
}

fn verify_node_io(
    ch_id: u32,
    node: &Node,
    io_conn: &mut IoConn,
    process_checker: &mut ProcessIOChecker,
) -> Result<()> {
    let io_vec = io_conn.fetch_io(ch_id)?;
    let node_id = node.get_id();
    let node_name = node.get_name();
    let tup_id = process_checker.fetch_srcid(node_id as _)?;
    let inps = process_checker.fetch_inputs(node_id as _)?;
    let outs = process_checker.fetch_outputs(node_id as _)?;
    let flags = process_checker.fetch_flags(node_id as _)?;
    let mut processed_io = std::collections::BTreeSet::new();
    for (file_node, ty) in io_vec.iter() {
        if !processed_io.insert((file_node.clone(), *ty)) {
            continue;
        }
        if *ty == EventType::Read as _ || *ty == EventType::Open as _ {
            if input_matches_declared(&inps, &outs, file_node) {
                continue;
            }
            link_file_to_node(node_id, tup_id, process_checker, file_node)?;
        } else if *ty == EventType::Write as u8 {
            // Tasks do not declare outputs. Any observed write from a Task is ignored
            if node.get_type() == &RowType::Task {
                //link_output_to_task(node_id, process_checker, file_node)?;
                continue;
            }
            // Rules must write their declared outputs only.
            if output_matches_declared(&outs, file_node) {
                continue;
            }
            return Err(eyre!(
                "File {} being written was not an output to rule {}",
                file_node,
                node_name
            ));
        }
    }
    for inp in inps.iter() {
        let fname = inp.get_name();
        if declared_matches_input(&io_vec, fname) {
            continue;
        }
        if flags.contains('*') {
            eprintln!(
                "Proc:{} file {} was not read by rule {}",
                ch_id, fname, node_name
            );
        }
    }
    if node.get_type() == &RowType::Rule {
        for out in outs.iter() {
            let fname = match declared_matches_output(&io_vec, out) {
                Some(value) => value,
                None => continue,
            };
            eprintln!(
                "Proc:{} File {} was not written by rule {}",
                ch_id, fname, node_name
            );
            //return Err(eyre!("File {} was not written by rule {}",fname,rule_name));
        }
    }
    Ok(())
}

fn declared_matches_output<'a, 'b>(
    io_vec: &'a Vec<(String, u8)>,
    out: &'b Node,
) -> Option<&'b str> {
    let fname = out.get_name();
    for (node, ty) in io_vec.iter() {
        if ty == &(EventType::Write as u8) && node == fname {
            return None;
        }
    }
    Some(fname)
}

fn declared_matches_input(io_vec: &Vec<(String, u8)>, name: &str) -> bool {
    for (node, ty) in io_vec.iter() {
        if ty == &(EventType::Read as u8) || ty == &(EventType::Open as u8) && node == name {
            return true;
        }
    }
    false
}

fn output_matches_declared(outs: &Vec<Node>, file_node_name: &String) -> bool {
    for out in outs.iter() {
        if out.get_type().eq(&Excluded) {
            let exclude_pattern = out.get_name().to_string();
            let re = Regex::new(&*exclude_pattern).unwrap();
            if re.is_match(file_node_name) {
                return true;
            }
        }
        if out.get_name() == file_node_name {
            return true;
        }
    }
    false
}

fn link_file_to_node(
    rule_id: i64,
    tup_id: i64,
    process_checker: &mut ProcessIOChecker,
    file_node: &str,
) -> Result<()> {
    if let Ok(dirid) = process_checker.fetch_dirid(file_node) {
        let p = Path::new(file_node);
        if let Some(name) = p.file_name() {
            if let Ok(from_id) =
                process_checker.fetch_node_id(name.to_string_lossy().as_ref(), dirid)
            {
                process_checker.insert_link(from_id, rule_id, tup_id, false)?;
            }
        }
    }
    Ok(())
}


fn input_matches_declared(inps: &Vec<Node>, outs: &Vec<Node>, file_node: &String) -> bool {
    for inp in inps.iter().chain(outs.iter()) {
        if *inp.get_type() == RowType::Dir
            || *inp.get_type() == RowType::Group
            || *inp.get_type() == RowType::Env
            || *inp.get_type() == RowType::DirGen
        {
            continue;
        }
        if inp.get_name() == file_node {
            return true;
        }
    }
    false
}

use crate::eyre::WrapErr;
use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::slice::Iter;
use std::sync::Arc;
use std::thread;
use std::thread::yield_now;

use bimap::hash::BiHashMap;
use bimap::BiMap;
use crossbeam::channel::{Receiver, Select, SelectedOperation};
use ex::shell;
use eyre::{eyre, Result};
use incremental_topo::IncrementalTopo;
use indicatif::ProgressBar;
use log::debug;
use parking_lot::{Mutex, RwLock};
use regex::Regex;
use tupdb::db::{start_connection, Node, TupConnection, TupConnectionPool};
use tupetw::{DynDepTracker, EventHeader, EventType};
use tupparser::buffers::{BufferObjects, PathBuffers};
use tupparser::decode::decode_group_captures;
use tupparser::statements::{Loc, TupLoc};
use tupparser::TupPathDescriptor;

use crate::parse::{cancel_flag, ConnWrapper};
use crate::{TermProgress, IO_DB};
use tupdb::db::RowType;
use tupdb::db::RowType::Excluded;
use tupdb::error::{AnyError, DbResult};
use tupdb::inserts::LibSqlInserts;
use tupdb::queries::LibSqlQueries;

#[derive(Debug, Clone)]
struct PoisonedState {
    keep_going: bool,
    poisoned: Arc<RwLock<u8>>,
}

impl PoisonedState {
    fn new(keep_going: bool) -> Self {
        Self {
            keep_going,
            poisoned: Arc::new(RwLock::new(0)),
        }
    }
    fn is_poisoned(&self) -> bool {
        *self.poisoned.read() != 0 || cancel_flag().load(std::sync::atomic::Ordering::Relaxed)
    }
    fn update_poisoned(&self, poisoned: u8) -> bool {
        if !self.is_poisoned() {
            *self.poisoned.write() = poisoned;
            true
        } else {
            false
        }
    }
    fn force_poisoned(&self) {
        *self.poisoned.write() = 1;
    }
    fn should_stop(&self) -> bool {
        if cancel_flag().load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
        let p = self.poisoned.read();
        *p == 1 && (!self.keep_going || *p > 1)
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
            dag.add_dependency(node1, node2).map_err(|e| {
                term_progress.abandon(&pbar, "Cyclic dependency detected!");
                AnyError::from(e.to_string())
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
    pub keep_going: bool,
    pub term_progress: TermProgress,
    pub num_jobs: usize,
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
        if let Some(id) = conn.fetch_dirid_by_path(path.as_path()).ok() {
            dir_ids.push(id);
        }
    }
    Ok(dir_ids)
}
fn execute_rule(
    rule_node: &Node,
    dirpath: &Path,
    spawned_child_id_sender: &crossbeam::channel::Sender<u32>,
    children: &mut Vec<Arc<Mutex<(Child, String)>>>,
) -> Result<()> {
    let mut cmd = shell(rule_node.get_name());
    cmd.current_dir(dirpath);

    let ch = cmd.spawn()?;
    let ch_id = ch.id();
    if rule_node.get_display_str().is_empty() {
        println!("id:{} {:?}", ch_id, cmd);
    } else {
        println!("{} {}", ch_id, rule_node.get_display_str());
    }
    spawned_child_id_sender.send(ch_id)?;

    children.push(Arc::new(Mutex::new((ch, rule_node.get_name().to_owned()))));
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
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
    let conn = connection_pool.get()?;
    let target_ids = get_target_ids(&conn, root, target)?;
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
    let mut pbars: Vec<ProgressBar> = Vec::new();
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
        .num_threads(num_threads)
        .build()
        .expect("Failed to build thread pool to execute rules");
    let _ = thread_pool.scope(|s| -> Result<()> {
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
                if poisoned.should_stop() {
                    let rule_id = rule_node.get_id();
                    let rule_name = rule_node.get_name();
                    completed_child_id_sender.send((0, (rule_id, rule_name.to_owned(), false)))?;
                    for pb in pbars {
                        pb.abandon();
                    }
                    eyre::bail!("Aborted executing rule: \n{}", rule_name);
                } else {
                    execute_rule(
                        rule_node,
                        &dirpaths[j],
                        &spawned_child_id_sender,
                        &mut children[j % num_threads],
                    )?;
                }
            }

            let wg = crossbeam::sync::WaitGroup::new();
            for i in 0..num_threads {
                let poisoned = poisoned.clone();
                let i = i.clone();
                let children = children[i].clone();
                // here we wait for tasks to finish
                let completed_child_id_sender = completed_child_id_sender.clone();
                let rule_for_child_id = rule_for_child_id.clone();
                let wg = wg.clone();
                let pbar = term_progress.make_len_progress_bar("..", children.len() as u64);
                pbars.push(pbar.clone());
                s.spawn(move |_| {
                    let (finished, failed) =
                        wait_for_children(poisoned, children, &pbar, &term_progress);
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
                    pbar.finish();
                    drop(wg);
                });
            }
            wg.wait(); // wait for all processes to finish before next topo order rules are executed
            min_idx = max_idx;

            if poisoned.should_stop() {
                for pb in pbars {
                    pb.finish();
                }
                eyre::bail!("Stopping further rule executions");
            }
        } // min_topo_order..max topo order
        drop(completed_child_id_sender);
        drop(spawned_child_id_sender);
        Ok(())
    });
    Ok(())
}

fn kill_poisoned(children: &Vec<Arc<Mutex<(Child, String)>>>) -> Vec<u32> {
    let mut failed_children = Vec::new();
    children.into_iter().for_each(|ch| {
        let ref mut ch = ch.lock().0;
        let id = ch.id();
        failed_children.push(id);
        ch.kill().ok();
        eprintln!("Killed child process {}", id);
    });
    failed_children
}

fn wait_for_children(
    poisoned: PoisonedState,
    mut children: Vec<Arc<Mutex<(Child, String)>>>,
    pbar: &ProgressBar,
    term_progress: &TermProgress,
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
                    if poisoned.update_poisoned(2) {
                        yield_now();
                    }
                } else {
                    debug!("finished executing rule: \n{:?}", ch.1);
                    finished.push(id);
                }
                term_progress.progress(pbar);
            } else {
                tryagain.push(child.clone());
            }
        }
        children = tryagain;
        yield_now();
        if !children.is_empty() {
            thread::sleep(std::time::Duration::from_nanos(100));
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

    fn insert_link(&mut self, from_id: i64, to_id: i64, is_output: bool) -> Result<()> {
        self.conn
            .insert_link(from_id, to_id, is_output.into(), RowType::Rule)?;
        Ok(())
    }

    fn upsert_output_producer(
        &mut self,
        output_id: i64,
        producer_id: i64,
        producer_type: RowType,
    ) -> Result<()> {
        self.conn
            .upsert_output_producer_link(output_id, producer_id, producer_type)?;
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
    let io_conn_pool = start_connection(IO_DB, 2).expect("Failed to open in memory db");
    let mut io_conn = io_conn_pool.get()?;
    tupdb::db::create_dyn_io_temp_tables(&mut io_conn)?;
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
                    eprintln!("Error verifying rule io {}\n{}", rule_name, e.to_string());
                    poisoned.update_poisoned(2);
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
            link_file_to_node(node_id, process_checker, file_node)?;
        } else if *ty == EventType::Write as u8 {
            // Tasks do not declare outputs. Any observed write from a Task is considered its output.
            if node.get_type() == &RowType::Task {
                link_output_to_task(node_id, process_checker, file_node)?;
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
    process_checker: &mut ProcessIOChecker,
    file_node: &str,
) -> Result<()> {
    if let Ok(dirid) = process_checker.fetch_dirid(file_node) {
        let p = Path::new(file_node);
        if let Some(name) = p.file_name() {
            if let Ok(from_id) =
                process_checker.fetch_node_id(name.to_string_lossy().as_ref(), dirid)
            {
                process_checker.insert_link(from_id, rule_id, false)?;
            }
        }
    }
    Ok(())
}

fn link_output_to_task(
    task_id: i64,
    process_checker: &mut ProcessIOChecker,
    file_node: &str,
) -> Result<()> {
    if let Ok(dirid) = process_checker.fetch_dirid(file_node) {
        let p = Path::new(file_node);
        if let Some(name) = p.file_name() {
            // Try to find existing node id; if not found, insert a GenF node first.
            let output_id =
                match process_checker.fetch_node_id(name.to_string_lossy().as_ref(), dirid) {
                    Ok(id) => {
                        // Best-effort: if existing node has no srcid, set it to this task (treat srcid as generic producer)
                        if let Ok(out_node) = process_checker.fetch_node_by_id(id) {
                            if out_node.get_srcid() <= 0 {
                                use tupdb::inserts::LibSqlInserts as _;
                                let _ = process_checker.conn.update_srcid(id, task_id).map_err(
                                    |e| {
                                        eyre!(
                                            "Failed to set srcid for task output '{}': {}",
                                            file_node,
                                            e
                                        )
                                    },
                                )?;
                            }
                        }
                        id
                    }
                    Err(_) => {
                        // Create a GenF node with srcid = task_id (treat srcid as generic producer)
                        let gen_node = Node::new_file_or_genf(
                            -1,
                            dirid,
                            0,
                            name.to_string_lossy().to_string(),
                            RowType::GenF,
                            task_id,
                        );
                        // Upsert node into DB so we can link to it
                        use tupdb::inserts::LibSqlInserts as _;
                        let inserted = process_checker
                            .conn
                            .fetch_upsert_node(&gen_node, || String::new())
                            .map_err(|e| {
                                eyre!("Failed to create task output node '{}': {}", file_node, e)
                            })?;
                        inserted.get_id()
                    }
                };
            // Create Producer (Task) -> Output edge with uniqueness enforced globally (Rule or Task)
            process_checker.upsert_output_producer(output_id, task_id, RowType::Task)?;
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

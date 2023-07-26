use std::collections::HashMap;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::slice::Iter;
use std::sync::Arc;
use std::thread;
use std::thread::yield_now;

use bimap::hash::BiHashMap;
use bimap::BiMap;
use crossbeam::channel::Receiver;
use ex::shell;
use eyre::{eyre, Result};
use incremental_topo::IncrementalTopo;
use log::debug;
use parking_lot::{Mutex, RwLock};
use rusqlite::Connection;

use tupetw::{DynDepTracker, EventHeader, EventType};

use crate::db::RowType::Excluded;
use crate::db::{ForEachClauses, LibSqlExec, LibSqlPrepare, Node, RowType, SqlStatement};

pub fn prepare_for_execution(
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

pub fn execute_targets(target: &Vec<String>, keep_going: bool, root: PathBuf) -> Result<()> {
    let mut conn = Connection::open(".tup/db")
        .expect("Connection to tup database in .tup/db could not be established");
    let (dag, node_bimap) = prepare_for_execution(&mut conn)?;

    //create_dyn_io_temp_tables(&conn)?;
    // start tracking file io by subprocesses.
    let rule_nodes = conn.rules_to_run_no_target()?;
    if rule_nodes.is_empty() {
        println!("Nothing to do");
        return Ok(());
    }
    let _ = exec_rules_to_run(
        conn,
        rule_nodes,
        &node_bimap,
        &dag,
        root.as_path(),
        &target,
        keep_going,
    )?;
    Ok(())
}

pub fn exec_rules_to_run(
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

    let rule_for_child_id = Arc::new(RwLock::new(std::collections::BTreeMap::new()));
    let mut tracker = DynDepTracker::build(std::process::id(), trace_sender);
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
                    eyre::bail!("Aborted executing rule: \n{}", rule_name)
                }
                let mut cmd = shell(rule_node.get_name());
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
                let wg = crossbeam::sync::WaitGroup::new();
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

fn kill_poisoned(
    poisoned: &Arc<RwLock<u8>>,
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
) -> Result<(Vec<u32>, Vec<u32>)> {
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
            thread::sleep(std::time::Duration::from_nanos(100));
            if kill_poisoned(&poisoned, &children, _keep_going) {
                break;
            }
        }
    }
    Ok((finished, failed))
}

struct ProcessIOChecker<'a> {
    input_getter: SqlStatement<'a>,
    output_getter: SqlStatement<'a>,
    fetch_id_stmt: SqlStatement<'a>,
    fetch_dirid_stmt: SqlStatement<'a>,
    add_link_stmt: SqlStatement<'a>,
    rule_failed_stmt: SqlStatement<'a>,
}

struct IoConn<'b> {
    fetch_io_statement: SqlStatement<'b>,
    insert_trace_statement: SqlStatement<'b>,
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

    fn reverify(&mut self, to_verify: Vec<(u32, i64, String)>) {
        self.to_verify = to_verify;
    }

    fn add(&mut self, child_id: u32, rule_id: i64, rule_name: String) {
        self.to_verify.push((child_id, rule_id, rule_name));
    }
}

impl<'a> ProcessIOChecker<'a> {
    fn new(conn: &'a mut Connection) -> Result<Self> {
        let s = Self {
            input_getter: conn.fetch_inputs_for_rule_prepare()?,
            output_getter: conn.fetch_outputs_for_rule_prepare()?,
            fetch_id_stmt: conn.fetch_nodeid_prepare()?,
            fetch_dirid_stmt: conn.fetch_dirid_prepare()?,
            add_link_stmt: conn.insert_link_prepare()?,
            rule_failed_stmt: conn.mark_rule_failed_prepare()?,
        };
        return Ok(s);
    }

    fn fetch_inputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        let fetch_inputs = self.input_getter.fetch_inputs(rule_id)?;
        Ok(fetch_inputs)
    }
    fn fetch_outputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        let fetch_outputs = self.output_getter.fetch_outputs(rule_id)?;
        Ok(fetch_outputs)
    }

    fn fetch_dirid<P: AsRef<Path>>(&mut self, node_path: P) -> Result<i64> {
        let dirid = self.fetch_dirid_stmt.fetch_dirid(node_path)?;
        Ok(dirid)
    }
    fn fetch_node_id(&mut self, node_name: &str, dirid: i64) -> Result<i64> {
        let nodeid = self.fetch_id_stmt.fetch_node_id(node_name, dirid)?;
        Ok(nodeid)
    }

    fn mark_failed(&mut self, rule_id: i64) -> Result<()> {
        self.rule_failed_stmt.mark_rule_failed(rule_id)?;
        Ok(())
    }

    fn insert_link(&mut self, from_id: i64, rule_id: i64) -> Result<()> {
        self.add_link_stmt
            .insert_link(from_id, rule_id, false, RowType::Rule)?;
        Ok(())
    }
}

impl<'b> IoConn<'b> {
    fn new(conn: &'b mut Connection) -> Result<Self> {
        let s = Self {
            fetch_io_statement: conn.fetch_io_prepare()?,
            insert_trace_statement: conn.insert_trace_prepare()?,
        };
        return Ok(s);
    }

    fn fetch_io(&mut self, child_id: u32) -> Result<Vec<(String, u8)>> {
        let fetch_io = self.fetch_io_statement.fetch_io(child_id as _)?;
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
                self.insert_trace_statement.insert_trace(
                    file_path.as_str(),
                    process_id as _,
                    process_gen,
                    event_type as u8,
                    child_cnt,
                )?
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

                        self.insert_trace_statement.insert_trace(
                            rel_path.as_path(),
                            parent_process_id as _,
                            process_gen as _,
                            event_type as u8,
                            child_cnt,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }
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
) -> Result<()> {
    let mut io_conn =
        Connection::open(root.join(".tup/io.db")).expect("Failed to open in memory db");
    crate::db::create_dyn_io_temp_tables(&mut io_conn)?;
    let mut end_completed_child_ids = false;
    let mut end_spawned_child_ids = false;
    let mut end_trace = false;
    let mut process_checker = ProcessIOChecker::new(conn)?;
    let mut deleted_child_procs = std::collections::BTreeSet::new();

    let mut to_verify = RulesToVerify {
        to_verify: Vec::new(),
    };
    let mut io_conn = IoConn::new(&mut io_conn)?;
    let mut children = std::collections::BTreeSet::new();
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
        while let Ok(oper) = sel.try_select() {
            if !keep_going && *poisoned.read() != 0 {
                break;
            }
            match oper.index() {
                i if i == index_child_ids => {
                    if let Ok((child_id, (rule_id, rule_name, succeeded))) =
                        oper.recv(&child_id_receiver).map_err(|_| {
                            debug!("no more children  expected");
                            end_completed_child_ids = true;
                        })
                    {
                        handle_childids(
                            &mut process_checker,
                            child_id,
                            rule_id,
                            rule_name,
                            succeeded,
                            &mut to_verify,
                        )?;
                    }
                }
                i if i == index_trace => {
                    if let Ok(evt_header) = oper.recv(&trace_receiver).map_err(|_| {
                        debug!("no more trace events expected");
                        end_trace = true;
                    }) {
                        handle_trace(root, &mut io_conn, &mut deleted_child_procs, evt_header);
                    }
                }
                i if i == index_spawned_child => {
                    if let Ok(child_id) = oper.recv(&spawned_child_id_receiver).map_err(|_| {
                        debug!("no more spawned child ids expected");
                        end_spawned_child_ids = true;
                    }) {
                        debug!("spawned child id recvd :{}", child_id);
                        children.insert(child_id);
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
            let mut reverify = Vec::new();
            for (child_id, rule_id, rule_name) in to_verify.iter() {
                if !deleted_child_procs.contains(&child_id) {
                    reverify.push((*child_id, *rule_id, rule_name.clone()));
                    continue;
                }
                deleted_child_procs.remove(&child_id);
                if let Err(e) = verify_rule_io(
                    *child_id,
                    *rule_id as _,
                    rule_name.as_str(),
                    &mut io_conn,
                    &mut process_checker,
                ) {
                    eprintln!("Error verifying rule io {}\n{}", rule_name, e.to_string());
                    process_checker
                        .mark_failed(*rule_id as _)
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
            to_verify.reverify(reverify);
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

fn handle_trace(
    root: &Path,
    //io_conn: &mut Connection,
    io_conn: &mut IoConn,
    deleted_child_procs: &mut std::collections::BTreeSet<u32>,
    evt_header: EventHeader,
) {
    //dir_children_set.insert(p);
    let process_id = evt_header.get_process_id();
    let event_type = evt_header.get_event_type();
    if event_type == EventType::ProcessDeletion as _ {
        deleted_child_procs.insert(process_id);
    }
    io_conn
        .insert_trace(root, &evt_header)
        .expect("Failed to insert trace");
}

fn handle_childids(
    process_checker: &mut ProcessIOChecker,
    child_id: u32,
    rule_id: i64,
    rule_name: String,
    succeeded: bool,
    rules_to_verify: &mut RulesToVerify,
) -> Result<()> {
    log::info!(
        "Child id {} finished executing rule: {}",
        child_id,
        rule_name
    );
    if succeeded {
        rules_to_verify.add(child_id, rule_id, rule_name);
    } else {
        debug!("Error running rule: {}", rule_name);
        process_checker.mark_failed(rule_id as _).map_err(|e| {
            eyre!(
                "Could not write failed rule {} with id :{} to db, \n {}",
                rule_name,
                rule_id,
                e.to_string()
            )
        })?;
    }
    Ok(())
}

fn verify_rule_io(
    ch_id: u32,
    rule_id: i32,
    rule_name: &str,
    io_conn: &mut IoConn,
    process_checker: &mut ProcessIOChecker,
) -> Result<()> {
    let io_vec = io_conn.fetch_io(ch_id)?;
    let inps = process_checker.fetch_inputs(rule_id as _)?;
    let outs = process_checker.fetch_outputs(rule_id as _)?;
    let mut processed_io = std::collections::BTreeSet::new();
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

            if let Ok(dirid) = process_checker.fetch_dirid(fnode) {
                let p = Path::new(fnode);
                if let Some(name) = p.file_name() {
                    if let Ok(from_id) =
                        process_checker.fetch_node_id(name.to_string_lossy().as_ref(), dirid)
                    {
                        process_checker.insert_link(from_id, rule_id as _)?;
                    }
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

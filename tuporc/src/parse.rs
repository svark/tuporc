use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ops::Deref;
use std::path::Path;
use std::process::{Child, Stdio};
use std::sync::{Arc, RwLock};

use bimap::{BiHashMap, BiMap};
use crossbeam::sync::WaitGroup;
use eyre::{eyre, Report, Result};
use incremental_topo::IncrementalTopo;
use log::debug;
use num_cpus;
use parking_lot::Mutex;
use rusqlite::Connection;

use tupetw::EventType;
use tupparser::{
    Artifacts, GroupPathDescriptor, InputResolvedType, PathDescriptor, ReadWriteBufferObjects,
    ResolvedLink, RuleDescriptor, TupParser, TupPathDescriptor,
};
use tupparser::decode::{
    GlobPath, MatchingPath, OutputHandler, OutputHolder, PathBuffers, PathSearcher, RuleRef,
};
use tupparser::errors::Error;

use crate::{get_dir_id, LibSqlPrepare, MAX_THRS_DIRS, Node, RowType};
use crate::db::{
    create_path_buf_temptable,
    ForEachClauses, LibSqlExec, MiscStatements, SqlStatement,
};
use crate::db::RowType::{Env, GenF};
use crate::RowType::Rule;

// CrossRefMaps maps paths, groups and rules discovered during parsing with those found in database
// These are two ways maps, so you can query both ways
#[derive(Debug, Clone, Default)]
pub struct CrossRefMaps {
    gbo: BiMap<GroupPathDescriptor, i64>,
    pbo: BiMap<PathDescriptor, i64>,
    rbo: BiMap<RuleDescriptor, i64>,
    dbo: BiMap<TupPathDescriptor, i64>,
    ebo: BiMap<String, i64>,
}

impl CrossRefMaps {
    pub fn get_group_db_id(&self, g: &GroupPathDescriptor) -> Option<i64> {
        self.gbo.get_by_left(g).copied()
    }
    pub fn get_path_db_id(&self, p: &PathDescriptor) -> Option<i64> {
        self.pbo.get_by_left(p).copied()
    }
    pub fn get_rule_db_id(&self, r: &RuleDescriptor) -> Option<i64> {
        self.rbo.get_by_left(r).copied()
    }

    pub fn get_tup_dir(&self, r: &TupPathDescriptor) -> Option<i64> {
        self.dbo.get_by_left(r).copied()
    }

    pub fn get_env_db_id(&self, e: &String) -> Option<i64> {
        self.ebo.get_by_left(e).copied()
    }

    pub fn add_group_xref(&mut self, g: GroupPathDescriptor, db_id: i64) {
        self.gbo.insert(g, db_id);
    }
    pub fn add_env_xref(&mut self, e: String, db_id: i64) {
        self.ebo.insert(e, db_id);
    }

    pub fn add_path_xref(&mut self, p: PathDescriptor, db_id: i64) {
        self.pbo.insert(p, db_id);
    }
    pub fn add_rule_xref(&mut self, r: RuleDescriptor, db_id: i64) {
        self.rbo.insert(r, db_id);
    }
    pub fn add_tup_dir(&mut self, t: TupPathDescriptor, db_id: i64) {
        self.dbo.insert(t, db_id);
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
        let diff_path = ph.get_rel_path(glob_path.get_path_desc(), glob_path.get_base_desc());
        let fetch_row = |s: &String| -> Option<MatchingPath> {
            debug!("found:{} at {:?}", s, base_path.as_path());
            let (pd, _) = ph.add_path_from(base_path.as_path(), Path::new(s.as_str()));
            if has_glob_pattern {
                let full_path = glob_path.get_base_abs_path().join(s.as_str());
                if glob_path.is_match(full_path.as_path()) {
                    let grps = glob_path.group(full_path.as_path());
                    Some(MatchingPath::with_captures(pd, grps))
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
                    let dir = tupfile.get_pid();
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
            crossref.add_tup_dir(tupid, tup_node.get_pid());
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

    // now check if the outputs (deleted or modified) trigger any other rules
    // and if so, add them to the list of tupfiles to be processed

    //let rules = gather_rules_to_run(conn)?;

    Ok(Vec::new())
}

fn add_link_glob_dir_to_rules(
    conn: &mut Connection,
    rw_buf: &ReadWriteBufferObjects,
    arts: &Artifacts,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut insert_link = tx.insert_link_prepare()?;
        let mut links_to_add = HashMap::new();
        for rlink in arts.get_resolved_links() {
            let rule_id = crossref
                .get_rule_db_id(rlink.get_rule_desc())
                .expect("tupfile dir not found");
            rlink.for_each_glob_path_desc(|glob_path_desc| {
                let glob_dir_desc = rw_buf.get_parent_id(&glob_path_desc).unwrap();
                links_to_add.insert(glob_dir_desc, rule_id);
            });
        }
        for (glob_dir_desc, tupfile_dir) in links_to_add {
            let glob_dir = crossref
                .get_path_db_id(&glob_dir_desc)
                .unwrap_or_else(|| panic!("glob dir not found:{:?} ", glob_dir_desc));
            insert_link.insert_link(glob_dir, tupfile_dir, true, RowType::Dir)?;
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
    conn: &Connection,
    fwd_refs: &BiMap<i32, incremental_topo::Node>,
    dag: &IncrementalTopo,
    root: &Path,
    target: &Vec<String>,
    keep_going: bool,
) -> Result<(VecDeque<u32>, Vec<Node>)> {
    let mut rule_nodes = conn.rules_to_run_no_target()?;
    // order the rules based on their dependencies
    let target_ids = conn.get_target_ids(root, target)?;
    let mut valid_rules = Vec::new();
    if target_ids.is_empty() {
        valid_rules = rule_nodes;
    } else {
        for r in rule_nodes.drain(..) {
            let id = r.get_id() as i32;
            let rule_node_in_dag = fwd_refs.get_by_left(&id).unwrap();
            for target_id in target_ids.iter() {
                let t = *target_id as i32;
                if let Some(target_node_in_dag) = fwd_refs.get_by_left(&t) {
                    if dag.contains_transitive_dependency(rule_node_in_dag, target_node_in_dag) {
                        valid_rules.push(r);
                        break;
                    }
                }
            }
        }
        if valid_rules.is_empty() {
            println!("No rules to run!");
            return Ok((VecDeque::new(), Vec::new()));
        }
    }
    let mut topo_order = HashMap::new();
    for r in valid_rules.iter() {
        let id = r.get_id() as i32;
        let rule_node_in_dag = fwd_refs.get_by_left(&id).unwrap();
        dag.descendants_unsorted(rule_node_in_dag)
            .map(|x| {
                x.for_each(|(order, ref dag_node)| {
                    let id = fwd_refs.get_by_right(dag_node).unwrap();
                    let o = topo_order.entry(id).or_insert(order);
                    if order.cmp(o) == std::cmp::Ordering::Greater {
                        *o = order;
                    }
                });
            })
            .expect(&*format!(
                "unable to sort rules to run from {}",
                r.get_name()
            ));
    }
    let descendant_count = topo_order.len();
    let mut max_topo_order = 0;
    let mut min_topo_oder = usize::MAX;
    for (_, v) in topo_order.iter() {
        max_topo_order = std::cmp::max(max_topo_order, *v);
        min_topo_oder = std::cmp::min(min_topo_oder, *v);
    }
    let mut childids = VecDeque::new();
    let poisoned = Arc::new(RwLock::new(0));
    let num_threads = std::cmp::min(num_cpus::get(), valid_rules.len());
    {
        let poisoned = poisoned.clone();
        ctrlc::set_handler(move || {
            if let Ok(mut poisoned) = poisoned.write() {
                *poisoned = 1_u8;
            }
        })
            .expect("Error setting Ctrl-C handler");
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
    let mut dirpath = conn.fetch_node_path_prepare()?;
    for r in valid_rules.iter() {
        let path = dirpath.fetch_node_path(r.get_name(), r.get_pid())?;
        dirpaths.push(path);
    }
    let mut min_idx = 0;
    /* let mut stream = termcolor::BufferedStandardStream::stdout(termcolor::ColorChoice::Always);
     stream.set_color(
         termcolor::ColorSpec::new()
             .set_fg(Some(termcolor::Color::Green))
             .set_bold(true),
     )
         .unwrap(); */

    for o in min_topo_oder..max_topo_order {
        let max_idx = valid_rules.partition_point(|x| {
            let id = x.get_id() as i32;
            topo_order.get(&id).unwrap().cmp(&o) == std::cmp::Ordering::Less
        }); // we limit ourselves to nodes with same topo order, so that dependent rules are run later

        let mut children = Vec::new();
        (0..num_threads).for_each(|_| {
            children.push(Vec::<Arc<Mutex<Child>>>::new());
        });
        for j in min_idx..max_idx {
            let rule_node = &valid_rules[j];
            if let Ok(poisoned) = poisoned.read() {
                if *poisoned == 1 {
                    return Err(eyre!("Aborted executing rule: \n{}", rule_node.get_name()));
                }
            }
            let mut cmd = execute::command(rule_node.get_name());
            cmd.current_dir(dirpaths[j].as_path());
            if rule_node.get_display_str().is_empty() {
                println!("{:?}", cmd);
            } else {
                println!("{}", rule_node.get_display_str());
            }
            let ch = cmd.spawn()?;
            let ch_id = ch.id();
            children[j % num_threads].push(Arc::new(Mutex::new(ch)));
            childids.push_back(ch_id);
            cmd.stdout(Stdio::piped());
            cmd.stderr(Stdio::piped());
        }

        crossbeam::scope(|s| -> Result<()> {
            for i in 0..num_threads {
                {
                    let poisoned = poisoned.clone();
                    let num_threads = num_threads.clone();
                    let i = i.clone();
                    let min_idx = min_idx.clone();
                    let max_idx = max_idx.clone();
                    let children = children[i].drain(..).collect::<Vec<_>>();
                    s.spawn(move |_| -> Result<()> {
                        poisoned.read()
                            .ok() // Convert Result to Option
                            .and_then(|guard| if *guard == 0 { Some(Ok(())) } else { None })
                            .unwrap_or_else(|| {
                                for j in (i + min_idx..max_idx).step_by(num_threads) {
                                    let ch = children[j].clone();
                                    ch.lock().kill()?;
                                }
                                Ok::<(), Report>(())
                            })
                    });
                }
                {
                    let poisoned = poisoned.clone();
                    let num_threads = num_threads.clone();
                    let i = i.clone();
                    let min_idx = min_idx.clone();
                    let max_idx = max_idx.clone();
                    let children = children[i].clone();
                    let ref valid_rules = valid_rules;
                    s.spawn(move |_| -> Result<()> {
                        for j in (i + min_idx..max_idx).step_by(num_threads) {
                            let ref exit_status = children[j].lock().wait()?;
                            if !exit_status.success() {
                                if let Ok(mut poisoned) = poisoned.write() {
                                    if *poisoned == 0 {
                                        *poisoned = 2_u8;
                                    }
                                }
                                if !keep_going {
                                    //eprintln!("{}", String::from_utf8_lossy(&exit_status.stderr));
                                    return Err(eyre!(
                                    "Error executing rule: \n{:?}",
                                    valid_rules[j].get_name()
                                ));
                                }
                            }
                        }
                        Ok(())
                    });
                }
            }
            Ok(())
        })
            .unwrap()
            .expect(" panic message");
        min_idx = max_idx;

        if let Ok(poisoned) = poisoned.read() {
            if *poisoned != 0 {
                return Err(eyre!("Stopping further rule executions"));
            }
        }
    } // min_topo_order..max topo order
    Ok((childids, valid_rules))
}

pub(crate) fn verify_child_processes_io(
    conn: &Connection,
    rule_nodes: &Vec<Node>,
    childids: &VecDeque<u32>,
) -> Result<()> {
    let mut input_getter = conn.fetch_inputs_for_rule_prepare()?;
    let mut output_getter = conn.fetch_outputs_for_rule_prepare()?;
    let mut fetch_io_stmt = conn.fetch_io_prepare()?;
    for (i, r) in rule_nodes.iter().enumerate() {
        let ch_id = childids[i];
        let io_vec = fetch_io_stmt.fetch_io(ch_id as i32)?; // this will fail if the child process failed
        let inps = input_getter.fetch_inputs(r.get_id() as i32)?;
        let outs = output_getter.fetch_outputs(r.get_id() as i32)?;
        'outer: for (fnode, ty) in io_vec.iter() {
            if *ty == EventType::Read as u8 {
                let fnode = fnode.strip_prefix("./").unwrap_or(fnode.as_ref());
                for inp in inps.iter() {
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
                return Err(eyre!(
                    "File {} being read was not an input to rule {}",
                    fnode,
                    r.get_name()
                ));
            } else if *ty == EventType::Write as u8 {
                let fnode = fnode.strip_prefix("./").unwrap_or(fnode.as_ref());
                for out in outs.iter() {
                    if out.get_name() == fnode {
                        continue 'outer;
                    }
                }
                return Err(eyre!(
                    "File {} being written was not an output to rule {}",
                    fnode,
                    r.get_name()
                ));
            }
        }
        'outer2: for inp in inps.iter() {
            let fname = inp.get_name();
            let fname = fname.strip_prefix("./").unwrap_or(fname.as_ref());
            for (fnode, ty) in io_vec.iter() {
                if ty == &(EventType::Read as u8) && fnode == fname {
                    continue 'outer2;
                }
            }
            return Err(eyre!(
                "File {} was not read by rule {}",
                fname,
                r.get_name()
            ));
        }
        'outer3: for out in outs.iter() {
            let fname = out.get_name();
            let fname = fname.strip_prefix("./").unwrap_or(fname.as_ref());
            for (fnode, ty) in io_vec.iter() {
                if ty == &(EventType::Write as u8) && fnode == fname {
                    continue 'outer3;
                }
            }
            return Err(eyre!(
                "File {} was not written by rule {}",
                fname,
                r.get_name()
            ));
        }
    }
    Ok(())
}

pub(crate) fn prepare_for_execution(
    root: &Path,
) -> Result<(
    Connection,
    IncrementalTopo,
    BiHashMap<i32, incremental_topo::Node>,
)> {
    let mut conn = Connection::open(root.join(".tup/db"))
        .expect("Connection to tup database in .tup/db could not be established");

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
        conn,
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
        let db_id_of_o = crossref.get_path_db_id(o).unwrap_or_else(|| {
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
                let rule_node_id = crossref
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
                            if let Some(group_id) = crossref.get_group_db_id(&g) {
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
                            if let Some(pid) = crossref.get_path_db_id(mp.path_descriptor()) {
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
                            if let Some(pid) = crossref.get_path_db_id(&p) {
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
                            if let Some(group_id) = crossref.get_group_db_id(&g) {
                                debug!("group link {} => {}", group_id, rule_node_id);
                                if processed_group.insert(group_id) {
                                    inp_linker.insert_link(
                                        group_id,
                                        rule_node_id,
                                        true,
                                        RowType::Rule,
                                    )?;
                                }
                                if let Some(pid) = crossref.get_path_db_id(&p) {
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
                        let g = crossref
                            .get_group_db_id(g)
                            .unwrap_or_else(|| panic!("failed to fetch db id of group {:?}", g));
                        out_linker.insert_link(rule_node_id, g, true, RowType::Grp)?;
                        for t in rl.get_targets() {
                            let p = crossref
                                .get_path_db_id(t)
                                .unwrap_or_else(|| panic!("failed to fetch db id of path {}", t));
                            out_linker.insert_link(p, g, true, RowType::Grp)?;
                            out_linker.insert_link(rule_node_id, p, true, RowType::GenF)?;
                        }
                    } else {
                        for t in rl.get_targets() {
                            let p = crossref
                                .get_path_db_id(t)
                                .unwrap_or_else(|| panic!("failed to fetch db id of path {}", t));
                            out_linker.insert_link(rule_node_id, p, true, RowType::GenF)?;
                        }
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
            crossref.get_group_db_id(group_desc).unwrap_or_else(|| {
                panic!(
                    "could not fetch groupid from its internal id:{}",
                    group_desc
                )
            }),
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

fn find_by_path(path: &Path, find_stmt: &mut SqlStatement) -> Result<(i64, i64)> {
    let parent = path
        .parent()
        .unwrap_or_else(|| panic!("No parent folder found for file {:?}", path));

    let name = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| panic!("missing name:{:?}", path));
    let i = find_stmt.find_node_by_path(parent, name.as_str())?;
    Ok(i)
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
                    crossref.add_group_xref(*grp_id, i);
                    nodeids.insert((i, RowType::Grp));
                } else {
                    // gather groups that are not in the db yet.
                    let isz: usize = (*grp_id).into();
                    groups_to_insert.push(Node::new_grp(isz as i64, dir, grp_name));
                }
            }
        });
        let mut find_nodeid = conn.fetch_nodeid_prepare()?;
        let mut find_by_path_stmt = conn.find_node_by_path_prepare()?;

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
            if let Ok(nodeid) = find_nodeid.fetch_node_id(name.as_str(), dir) {
                crossref.add_rule_xref(*rule_desc, nodeid);
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
                    rules_to_insert.push(Node::new_rule(isz as i64, dir, name, display_str, flags));
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
        let mut collect_nodes_to_insert = |p: &PathDescriptor,
                                           rtype: &RowType,
                                           mtime_ns: i64,
                                           srcid: i64,
                                           crossref: &mut CrossRefMaps,
                                           find_nodeid: &mut SqlStatement|
                                           -> Result<()> {
            let isz: usize = (*p).into();
            let path = read_write_buf.get_path(p);
            //debug!("np:{:?}", path.as_path());
            let parent = path
                .as_path()
                .parent()
                .unwrap_or_else(|| panic!("No parent folder found for file {:?}", path.as_path()));
            let dir_desc = read_write_buf
                .get_parent_id(p)
                .unwrap_or_else(|| panic!("descriptor not found for path:{:?}", parent));
            let dir = {
                let x = find_dirid.fetch_dirid(parent);
                if x.is_err() {
                    debug!("failed to fetch dir id");
                }
                x?
            };
            crossref.add_path_xref(dir_desc, dir);

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
                crossref.add_path_xref(*p, nodeid);
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
        for r in arts.rules_by_tup().iter() {
            for rl in r.iter() {
                let rd = rl.get_rule_desc();
                let rule_ref = rl.get_rule_ref();
                let tup_desc = rule_ref.get_tupfile_desc();
                let dir = crossref.get_tup_dir(tup_desc).ok_or_else(|| {
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
                            if let Ok((nodeid, dirid)) =
                                find_by_path(Path::new(inp.as_str()), &mut find_by_path_stmt)
                            {
                                let parent_id = read_write_buf
                                    .get_parent_id(p)
                                    .unwrap_or_else(|| panic!("no parent id found for:{:?}", p));
                                crossref.add_path_xref(*p, nodeid);
                                crossref.add_path_xref(parent_id, dirid);
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
        let mut insert_node = tx.insert_node_prepare()?;
        let mut find_node = tx.fetch_node_prepare()?;
        let mut update_mtime = tx.update_mtime_prepare()?;
        let mut add_to_present = tx.add_to_present_prepare()?;
        let mut add_to_modify = tx.add_to_modify_prepare()?;
        for node in groups_to_insert
            .into_iter()
            .chain(paths_to_insert.into_iter())
            .into_iter()
            .chain(rules_to_insert.into_iter())
        {
            let desc = node.get_id() as usize;
            let db_id = find_upsert_node(
                &mut insert_node,
                &mut find_node,
                &mut update_mtime,
                &mut add_to_present,
                &mut add_to_modify,
                &node,
            )?
            .get_id();
            nodeids.insert((db_id, *node.get_type()));
            if RowType::Grp.eq(node.get_type()) {
                crossref.add_group_xref(GroupPathDescriptor::new(desc), db_id);
            } else if Rule.eq(node.get_type()) {
                crossref.add_rule_xref(RuleDescriptor::new(desc), db_id);
            } else {
                crossref.add_path_xref(PathDescriptor::new(desc), db_id);
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
            } else {
                let env_id = inst_env_stmt.insert_env_exec(env_var.as_str(), env_val.as_str())?;
                crossref.add_env_xref(env_var, env_id);
                add_to_modify_env_stmt.add_to_modify_exec(env_id, Env)?;
            }
        }
        for n in nodeids.iter() {
            tx.remove_id_from_delete_list(n.0)?;
        }
        tx.prune_present_list()?; // removes deletelist entries from present
        tx.prune_modified_list()?; // removes deletelist entries from modified
        //conn.remove_presents_prepare()?.remove_presents_exec()?;
    }
    tx.commit()?;
    Ok(nodeids)
}

// this is pretends to be the sqlite upsert operation
// it also adds the node to the present list, modify list and updates nodes mtime
pub(crate) fn find_upsert_node(
    insert_node: &mut SqlStatement,
    find_node_id: &mut SqlStatement,
    update_mtime: &mut SqlStatement,
    add_to_present: &mut SqlStatement,
    add_to_modify: &mut SqlStatement,
    node: &Node,
) -> Result<Node> {
    debug!("find_upsert_node:{} in dir:{}", node.get_name(), node.get_pid());
    let db_node = find_node_id
        .fetch_node(node.get_name(), node.get_pid())
        .and_then(|existing_node| {
            if (existing_node.get_mtime() - node.get_mtime()).abs() > 1 {
                debug!("updating mtime for:{}, {} -> {}", existing_node.get_name(), existing_node.get_mtime(), node.get_mtime());
                update_mtime.update_mtime_exec(existing_node.get_id(), node.get_mtime())?;
                add_to_modify
                    .add_to_modify_exec(existing_node.get_id(), *existing_node.get_type())?;
            }
            add_to_present
                .add_to_present_exec(existing_node.get_id(), *existing_node.get_type())?;
            Ok(existing_node)
        })
        .or_else(|e| {
            if let Some(rusqlite::Error::QueryReturnedNoRows) = e.root_cause().downcast_ref::<rusqlite::Error>() {
                let node = insert_node
                    .insert_node_exec(node)
                    .map(|i| Node::copy_from(i, node))?;
                add_to_modify.add_to_modify_exec(node.get_id(), *node.get_type())?;
                add_to_present.add_to_present_exec(node.get_id(), *node.get_type())?; // add to present list
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
                if let Some(group_db_id) = crossref.get_group_db_id(group_id) {
                    for target in rl.get_targets() {
                        if let Some(path_db_id) = crossref.get_path_db_id(target) {
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

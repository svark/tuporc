use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use anyhow::Result;
use bimap::BiMap;
use execute::shell;
use log::debug;
use rusqlite::Connection;

use tupparser::{
    Artifacts, GroupPathDescriptor, InputResolvedType, PathDescriptor, ReadWriteBufferObjects,
    ResolvedLink, RuleDescriptor, TupParser,
};
use tupparser::decode::{GlobPath, MatchingPath, OutputHandler, OutputHolder, PathBuffers, PathSearcher, RuleRef};
use tupparser::errors::Error;

use crate::{get_dir_id, LibSqlPrepare, Node, RowType};
use crate::db::{
    create_dir_path_buf_temptable, create_group_path_buf_temptable, create_tup_path_buf_temptable,
    ForEachClauses, LibSqlExec, SqlStatement,
};
use crate::db::RowType::GenF;
use crate::RowType::Rule;

// CrossRefMaps maps paths, groups and rules discovered during parsing with those found in database
// These are two ways maps, so you can query both ways
#[derive(Debug, Clone, Default)]
pub struct CrossRefMaps {
    gbo: BiMap<GroupPathDescriptor, i64>,
    pbo: BiMap<PathDescriptor, i64>,
    rbo: BiMap<RuleDescriptor, i64>,
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

    pub fn add_group_xref(&mut self, g: GroupPathDescriptor, db_id: i64) {
        self.gbo.insert(g, db_id);
    }
    pub fn add_path_xref(&mut self, p: PathDescriptor, db_id: i64) {
        self.pbo.insert(p, db_id);
    }
    pub fn add_rule_xref(&mut self, r: RuleDescriptor, db_id: i64) {
        self.rbo.insert(r, db_id);
    }
}

struct DbPathSearcher {
    conn: Connection,
    //glob_query: std::cell::RefCell<SqlStatement<'a>>,
    psx: OutputHolder,
}

impl DbPathSearcher {
    pub fn new(conn: Connection) -> DbPathSearcher {
        DbPathSearcher {
            conn,
            psx: OutputHolder::new(),
        }
    }

    fn fetch_glob_nodes(
        &self,
        ph: &mut (impl PathBuffers + Sized),
        glob_path: &GlobPath,
    ) -> Result<Vec<MatchingPath>> {
        let has_glob_pattern = glob_path.has_glob_pattern();
        let base_path = ph.get_path(glob_path.get_base_desc()).clone();
        let diff_path = ph.get_rel_path(glob_path.get_path_desc(), glob_path.get_base_desc());
        let fetch_row = |s: &String| {
            debug!("found:{} at {:?}", s, base_path.as_path());
            let (pd, _) = ph.add_path_from(base_path.as_path(), Path::new(s.as_str()));
            if has_glob_pattern {
                let grps = glob_path.group(s.as_str());
                MatchingPath::with_captures(pd, grps)
            } else {
                MatchingPath::new(pd)
            }
        };
        let mut glob_query = self.conn.fetch_glob_nodes_prepare()?;
        let mps = glob_query.fetch_glob_nodes(base_path.as_path(), diff_path.as_path(), fetch_row)?;
        Ok(mps)
    }
}

impl PathSearcher for DbPathSearcher {
    fn discover_paths(
        &self,
        ph: &mut impl PathBuffers,
        glob_path: &tupparser::decode::GlobPath,
    ) -> std::result::Result<Vec<MatchingPath>, Error> {
        let mps = self
            .fetch_glob_nodes(ph, glob_path)
            .and_then(|mut x| {
                x.extend(self.get_outs().discover_paths(ph, glob_path)?);
                Ok(x)
            })
            .map_err(|e| Error::new_path_search_error(e.to_string().as_str(), RuleRef::default()));
        mps
    }
    fn get_outs(&self) -> &OutputHolder {
        &self.psx
    }

    fn merge(&mut self, o: &impl OutputHandler) -> Result<(), Error> {
        OutputHandler::merge(&mut self.psx, o)
    }

    fn acquire(&mut self, t: OutputHolder) {
        self.psx.acquire(t)
    }
}

/// handle the tup parse command which assumes files in db and adds rules and makes links joining input and output to/from rule statements
pub fn parse_tupfiles_in_db<P: AsRef<Path>>(
    tupfiles: Vec<Node>,
    root: P,
    e: bool,
) -> Result<Vec<ResolvedLink>> {
    let (arts, mut rwbufs, mut outs) = {
        let conn = Connection::open(".tup/db")
            .expect("Connection to tup database in .tup/db could not be established");

        let db = DbPathSearcher::new(conn);
        let mut parser = TupParser::try_new_from(root.as_ref(), db)?;
        let arts = gather_rules_from_tupfiles(&mut parser, &tupfiles)?;
        let arts = parser.reresolve(arts)?;
        /*
        let dbref = parser.get_searcher();
        let mut del = dbref.conn.delete_tup_rule_links_prepare()?;
        let mut s = dbref.conn.fetch_rules_nodes_prepare_by_dirid()?;
        for tupfile in &tupfiles {
            let dir = tupfile.get_pid();
            let rules = s.fetch_rule_nodes_by_dirid(dir)?;
            for r in rules {
               //db.conn.
               del.delete_rule_links(r.get_id())?;
            }
            //del.add_to_delete_exec()
        } */
        (arts, parser.read_write_buffers(), parser.get_outs().clone())
    };
    let mut conn = Connection::open(".tup/db")
        .expect("Connection to tup database in .tup/db could not be established");

    let mut crossref = CrossRefMaps::default();
    insert_nodes(
        &mut conn,
        tupfiles.as_slice(),
        &rwbufs,
        &arts,
        &mut crossref,
    )?;

    check_uniqueness_of_parent_rule(&mut conn, &rwbufs, &outs, &mut crossref)?;
    //XTODO: delete previous links from output files to groups
    add_links_to_groups(&mut conn, &arts, &crossref)?;
    fetch_group_provider_outputs(&mut conn, &mut rwbufs, &mut outs, &mut crossref)?;
    add_rule_links(&mut conn, &rwbufs, &arts, &mut crossref)?;
    //let rules = gather_rules_to_run(conn)?;
    if e {
        exec_rules_to_run(&mut conn, root.as_ref())?;
    }

    Ok(Vec::new())
}

pub fn gather_tupfiles(conn: &mut Connection) -> Result<Vec<Node>> {
    let mut tupfiles = Vec::new();
    create_dir_path_buf_temptable(conn)?;
    create_group_path_buf_temptable(conn)?;
    create_tup_path_buf_temptable(conn)?;
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
    for tupfile_node in tupfiles.iter() {
        // try fetching statements in this tupfile already in the database to avoid inserting same rules again
        debug!("parsing {}", tupfile_node.get_name());
        let arts = p.parse(tupfile_node.get_name())?;
        new_arts.extend(arts)?;
        //new_outputs.merge(&o)?;
        //   let dir_id = tupfile_node.get_pid();
        //    del_stmt.delete_rule_links(dir_id)?; // XTODO: Mark nodes as being deleted, track unreachable leaves in the graph that depend on this node.
        //db_rules.extend(conn.fetch_db_rules(dir_id)?);
        // need to mark generated nodes from rules of this tupfile as deleted unless resurrected below
        //rules_in_tup_file.push(ParsedLinks::new(tupfile_node.clone(), rlinks));
    }
    Ok(new_arts)
}
pub(crate) fn exec_rules_to_run(conn: &mut Connection, root: &Path) -> Result<()> {
    let rule_nodes = conn.rules_to_run()?;

    for rule_node in rule_nodes {
        let cmd = shell(rule_node.get_name());
        if rule_node.get_display_str().is_empty() {
            println!("{:?}", cmd);
        } else {
            println!("{}", rule_node.get_display_str());
        }
        let out = tupexec::Command::from_std_cmd(cmd)
            .outdir(root.join(".tup").as_os_str())
            .spawn()?;
        let exit_status = out.wait();
        if !exit_status.success() {
            break;
        }
    }
    Ok(())
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
                    return Err(anyhow::Error::msg(
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
        let mut inp_linker = tconn.insert_sticky_link_prepare()?;
        let mut out_linker = tconn.insert_link_prepare()?;
        for r in rules_in_tup_file {
            for rl in r {
                let rule_node_id = crossref
                    .get_rule_db_id(rl.get_rule_desc())
                    .expect("rule dbid fetch failed");
                let mut processed = std::collections::HashSet::new();
                let mut processed_group = std::collections::HashSet::new();
                for i in rl.get_sources() {
                    let mut added: bool = false;
                    match i {
                        InputResolvedType::UnResolvedGroupEntry(g) => {
                            if let Some(group_id) = crossref.get_group_db_id(&g) {
                                inp_linker.insert_sticky_link(group_id, rule_node_id)?;
                                added = true;
                            }
                        }
                        InputResolvedType::Deglob(mp) => {
                            if let Some(pid) = crossref.get_path_db_id(mp.path_descriptor()) {
                                debug!("slink {} => {}", pid, rule_node_id);
                                if processed.insert(pid) {
                                    inp_linker.insert_sticky_link(pid, rule_node_id)?;
                                }
                                added = true;
                            }
                        }
                        InputResolvedType::BinEntry(_, p) => {
                            if let Some(pid) = crossref.get_path_db_id(&p) {
                                debug!("bin slink {} => {}", pid, rule_node_id);
                                if processed.insert(pid) {
                                    inp_linker.insert_sticky_link(pid, rule_node_id)?;
                                }
                                added = true;
                            }
                        }
                        InputResolvedType::GroupEntry(g, p) => {
                            if let Some(group_id) = crossref.get_group_db_id(&g) {
                                debug!("group link {} => {}", group_id, rule_node_id);
                                if processed_group.insert(group_id) {
                                    inp_linker.insert_sticky_link(group_id, rule_node_id)?;
                                }
                                if let Some(pid) = crossref.get_path_db_id(&p) {
                                    if processed.insert(pid) {
                                        inp_linker.insert_sticky_link(pid, rule_node_id)?;
                                    }
                                }
                                added = true;
                            }
                        }
                        InputResolvedType::UnResolvedFile(f) => {
                            debug!("unresolved entry found during linking {}", f);
                        }
                    }
                    if !added {
                        let fname = rbuf.get_input_path_str(&i);

                        anyhow::ensure!(
                            false,
                            format!(
                                "could not add a link from input {} to ruleid:{}",
                                fname, rule_node_id
                            )
                        );
                    }
                }
                {
                    for t in rl.get_targets() {
                        let p = crossref
                            .get_path_db_id(t)
                            .unwrap_or_else(|| panic!("failed to fetch db id of path {}", t));
                        out_linker.insert_link(rule_node_id, p)?;
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

fn insert_nodes(
    conn: &mut Connection,
    tup_nodes: &[Node],
    read_write_buf: &ReadWriteBufferObjects,
    arts: &Artifacts,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let rules_in_tup_file = arts.rules_by_tup();
    let mut groups_to_insert: Vec<_> = Vec::new();
    let mut paths_to_insert = BTreeSet::new();
    let mut rules_to_insert = Vec::new();
    let mut paths_to_update: HashMap<i64, i64> = HashMap::new();
    // collect all un-added groups and add them in a single transaction.
    {
        let mut find_dirid = conn.fetch_dirid_prepare()?;
        let mut find_group_id = conn.fetch_groupid_prepare()?;
        read_write_buf.for_each_group(|(group_path, grp_id)| {
            let parent = tupparser::transform::get_parent_str(group_path.as_path());
            if let Some(dir) = get_dir_id(&mut find_dirid, parent) {
                let grp_name = group_path.file_name();
                let id = find_group_id.fetch_group_id(grp_name.as_str(), dir).ok();
                if let Some(i) = id {
                    // grp_db_id.insert(grp_id, i);
                    crossref.add_group_xref(*grp_id, i);
                } else {
                    // gather groups that are not in the db yet.
                    let isz: usize = (*grp_id).into();
                    groups_to_insert.push(Node::new_grp(isz as i64, dir, grp_name));
                }
            }
        });
        let mut find_nodeid = conn.fetch_nodeid_prepare()?;

        let mut collect_rule_nodes_to_insert =
            |rule_desc: &RuleDescriptor,
             dir: i64,
             crossref: &mut CrossRefMaps,
             find_nodeid: &mut SqlStatement| {
                let isz: usize = (*rule_desc).into();
                let rule_formula = read_write_buf.get_rule(rule_desc);
                let name = rule_formula.get_rule_str();
                let display_str = rule_formula.get_display_str();
                let flags = rule_formula.get_flags();
                if let Ok(nodeid) = find_nodeid.fetch_node_id(name.as_str(), dir) {
                    crossref.add_rule_xref(*rule_desc, nodeid);
                } else {
                    rules_to_insert.push(Node::new_rule(isz as i64, dir, name, display_str, flags));
                }
            };
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
            let dir = {
                let x = find_dirid.fetch_dirid(parent);
                if x.is_err() {
                    debug!("failed to fetch dir id");
                }
                x?
            };
            let name = path
                .as_path()
                .file_name()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| panic!("missing name:{:?}", path.as_path()));
            if let Ok(nodeid) = find_nodeid.fetch_node_id(
                &name,
                dir,
            ) {
                //path_db_id.insert(p, nodeid);
                crossref.add_path_xref(*p, nodeid);
                paths_to_update.insert(nodeid, mtime_ns);
            } else {
                debug!("need to add {:?} in dir:{}", name, dir);
                paths_to_insert.insert(Node::new_file_or_genf(
                    isz as i64,
                    dir,
                    mtime_ns,
                    name,
                    *rtype,
                    srcid,
                ));
            }
            Ok(())
        };
        let mut processed = std::collections::HashSet::new();
        for r in rules_in_tup_file.iter().zip(tup_nodes.iter()) {
            let dir = r.1.get_id();
            let mtime = r.1.get_mtime();

            for rl in r.0.iter() {
                let rd = rl.get_rule_desc();
                collect_rule_nodes_to_insert(rd, dir, crossref, &mut find_nodeid);
                for p in rl.get_targets() {
                    if processed.insert(p) {
                        collect_nodes_to_insert(p, &GenF, mtime, dir, crossref, &mut find_nodeid)?;
                    }
                }
            }
            debug!("now gathering inputs to insert");
            for rl in r.0.iter() {
                for i in rl.get_sources() {
                    if let InputResolvedType::Deglob(mp) = i {
                        if processed.insert(mp.path_descriptor()) {
                            collect_nodes_to_insert(
                                mp.path_descriptor(),
                                &GenF,
                                mtime,
                                -1, /*srcid*/
                                crossref,
                                &mut find_nodeid,
                            )?;
                        }
                    }
                }
            }
        }
    }
    let tx = conn.transaction()?;
    {
        let mut insert_node = tx.insert_node_prepare()?;
        let mut find_node = tx.fetch_node_prepare()?;
        let mut update_mtime = tx.update_mtime_prepare()?;
        let mut add_to_present = tx.add_to_present_prepare()?;
        let mut add_to_modify = tx.add_to_modify_prepare()?;
        for node in groups_to_insert
            .into_iter()
            .chain(paths_to_insert.into_iter())
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
            if RowType::Grp.eq(node.get_type()) {
                crossref.add_group_xref(GroupPathDescriptor::new(desc), db_id);
            } else if Rule.eq(node.get_type()) {
                crossref.add_rule_xref(RuleDescriptor::new(desc), db_id);
            } else {
                crossref.add_path_xref(PathDescriptor::new(desc), db_id);
            }
        }
    }
    tx.commit()?;
    Ok(())
}

pub(crate) fn find_upsert_node(
    insert_node: &mut SqlStatement,
    find_node_id: &mut SqlStatement,
    update_mtime: &mut SqlStatement,
    add_to_present: &mut SqlStatement,
    add_to_modify: &mut SqlStatement,
    node: &Node,
) -> Result<Node> {
    debug!("find_upsert_node:{}", node.get_name());
    let db_node = find_node_id
        .fetch_node(node.get_name(), node.get_pid())
        .or_else(|_| {
            let node = insert_node
                .insert_node_exec(node)
                .map(|i| Node::copy_from(i, node))?;
            add_to_modify.add_to_modify_exec(node.get_id(), *node.get_type())?;
            Ok::<Node, anyhow::Error>(node)
        })
        .and_then(|existing_node| {
            if (existing_node.get_mtime() - node.get_mtime()).abs() > 2 {
                update_mtime.update_mtime_exec(existing_node.get_id(), node.get_mtime())?;
                add_to_modify
                    .add_to_modify_exec(existing_node.get_id(), *existing_node.get_type())?;
            } else {
                add_to_present
                    .add_to_present_exec(existing_node.get_id(), *existing_node.get_type())?;
            }
            Ok(existing_node)
        })?;
    Ok(db_node)
}

fn add_links_to_groups(
    conn: &mut Connection,
    arts: &Artifacts,
    crossref: &CrossRefMaps,
) -> Result<()> {
    let tx = conn.transaction()?;
    {
        let mut inp_linker = tx.insert_sticky_link_prepare()?;

        for rl in arts.get_resolved_links() {
            if let Some(group_id) = rl.get_group_desc().as_ref() {
                if let Some(group_db_id) = crossref.get_group_db_id(group_id) {
                    for target in rl.get_targets() {
                        if let Some(path_db_id) = crossref.get_path_db_id(target) {
                            inp_linker.insert_sticky_link(path_db_id, group_db_id)?;
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

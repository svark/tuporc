use crate::db::RowType::TupFType;
use crate::db::{
    create_dir_path_buf_temptable, create_group_path_buf_temptable, create_tup_path_buf_temptable,
    ForEachClauses, LibSqlExec,
};
use crate::RowType::RuleType;
use crate::{get_dir_id, is_tupfile, LibSqlPrepare, Node, RowType};
use anyhow::Result;
use daggy::{Dag, NodeIndex};
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tupparser::decode::{
    BufferObjects, GroupPathDescriptor, OutputTagInfo, PathDescriptor, ResolvePaths, ResolvedLink,
    TupPathDescriptor,
};
use tupparser::statements::{is_rule, Cat, LocatedStatement};
use tupparser::transform::{load_conf_vars, parse_tup, Deps};

pub struct ParsedStatements {
    tupfile: PathBuf,
    tupfile_node: Node,
    statements: Vec<LocatedStatement>,
}
impl ParsedStatements {
    pub fn new(tupfile: PathBuf, tupfile_node: Node, statements: Vec<LocatedStatement>) -> ParsedStatements
    {
        ParsedStatements{tupfile, tupfile_node, statements}
    }
    pub fn get_tupfile(&self) -> &Path {
        self.tupfile.as_path()
    }
    pub fn get_tupfile_node(&self) -> &Node {
        return &self.tupfile_node;
    }
    pub fn get_statement(&self, i: usize) -> &LocatedStatement { &self.statements[i]}
}

// handle the tup parse command which assumes files in db and adds rules and makes links joining input and output to/from rule statements
pub fn parse_tupfiles_in_db<P: AsRef<Path>>(
    conn: &mut Connection,
    root: P,
) -> Result<Vec<ResolvedLink>> {
    let mut tupfiles = Vec::new();
    create_dir_path_buf_temptable(conn)?;
    create_group_path_buf_temptable(conn)?;
    create_tup_path_buf_temptable(conn)?;
    //create_tup_outputs(conn)?;

    conn.for_each_tup_node_with_path(|i: i64, dirid: i64, entry: &Path| {
        if is_tupfile(entry.file_name().unwrap()) {
            let tupfilepath = entry.to_string_lossy().as_ref().to_string();
            // name stores full path here
            tupfiles.push(Node::new(i, dirid, 0, tupfilepath, TupFType));
        }
        Ok(())
    })?;

    let rootfolder = tupparser::parser::locate_file(root.as_ref(), "Tupfile.ini")
        .ok_or(tupparser::errors::Error::RootNotFound)?;
    let confvars = load_conf_vars(rootfolder.as_path())?;
    let mut bo = BufferObjects::default();
    let mut rules = Vec::new();
    let mut ids = Vec::new();

    let mut dag: Dag<u32, u32> = Dag::new();
    let mut provided_by: HashMap<_, Vec<_>> = HashMap::new();
    let mut required_by: HashMap<_, Vec<_>> = HashMap::new();
    let mut db_rules: Vec<Node> = Vec::new();
    {
        let mut del_stmt = conn.delete_tup_rule_links_prepare()?;
        let mut find_dirid = conn.fetch_dirid_prepare()?;
        for tupfile_node in tupfiles.iter() {
            // try fetching statements in this tupfile already in the database to avoid inserting same rules again
            ids.push(dag.node_count());
            let tup_node_name = tupfile_node.get_name();
            let tup_file_path = Path::new(tupfile_node.get_name());
            let stmts = parse_tup(&confvars, tup_node_name, &mut bo)?;
            //let tupdesc = TupPathDescriptor::new(tupfile_node.get_id() as usize);
            let tup_cwd = tup_file_path.parent().expect("tup file parent not found");
            let dir_id = find_dirid.fetch_dirid(tup_cwd)?;
            del_stmt.delete_rule_links(dir_id)?; // XTODO: Mark nodes as being deleted, track leaves left unreachable in the graph that depend on this node.
            db_rules.extend(conn.fetch_db_rules(dir_id)?);
            // need to mark generated nodes from rules of this tupfile as deleted unless resurrected below

            let mut groups = Vec::new();
            let mut bins = Vec::new();
            for l in stmts.iter().filter(|l| is_rule(l)) {
                l.output_groups(tup_cwd, &mut groups);
                groups.drain(..).for_each(|group| {
                    bo.get_mut_group_buffer_object().add_relative_group_by_path(Path::new(group.as_str()), tup_cwd);
                });
                l.input_groups(tup_cwd, &mut groups);
                groups.drain(..).for_each(|group| {
                    bo.get_mut_group_buffer_object().add_relative_group_by_path(Path::new(group.as_str()), tup_cwd);
                })
            }

            for l in stmts.iter().filter(|l| is_rule(l)) {
                let n = dag.add_node(1);
                l.input_groups(tup_cwd, &mut groups);
                groups.drain(..).for_each(|group| {
                    required_by
                        .entry("?G".to_owned() + group.as_str())
                        .or_default()
                        .push(n)
                });
                l.input_bins(tup_cwd, &mut bins);
                bins.drain(..).for_each(|bin| {
                    required_by
                        .entry("?B".to_owned() + bin.as_str())
                        .or_default()
                        .push(n)
                });
                l.output_groups(tup_cwd, &mut groups);
                groups.drain(..).for_each(|grp| {
                    provided_by
                        .entry("?G".to_owned() + grp.as_str())
                        .or_default()
                        .push(n)
                });
                l.output_bins(tup_cwd, &mut bins);
                bins.drain(..).for_each(|bin| {
                    provided_by
                        .entry("?B".to_owned() + bin.as_str())
                        .or_default()
                        .push(n)
                });
            }
            rules.push(ParsedStatements::new(tup_file_path.to_path_buf(), tupfile_node.clone(), stmts));
        }
    }

    let mut toinsert: Vec<_> = Vec::new();
    {
        let mut find_dirid = conn.fetch_dirid_prepare()?;
        let mut find_group_id = conn.fetch_groupid_prepare()?;
        for gp in bo.get_group_buffer_object().get_paths() {
            let parent = gp.as_path().parent().unwrap();
            if let Some(dir) = find_dirid.fetch_dirid(parent.to_string_lossy().to_string()).ok() {
                let id = find_group_id.fetch_groupid(gp.as_path()).ok();
                if id.is_none() {
                    let id = bo.get_group_buffer_object().try_get_id(gp).unwrap().clone();
                    let isz: usize = id.into();
                    toinsert.push(Node::new(isz as i64, dir, 0, gp.as_path().to_string_lossy().to_string(), RowType::GrpType));
                }
            }
        }
    }
    {
        let mut db_id: HashMap<GroupPathDescriptor, i64> = HashMap::new();
        let tx = conn.transaction()?;
        let mut insert_grp = tx.insert_node_prepare()?;
        for node in toinsert {
            let grpid = insert_grp.insert_node_exec(&node)?;
            db_id.insert(GroupPathDescriptor::new(node.get_id() as usize), grpid);
        }
        //tx.commit()?;
    }

    ids.push(dag.node_count());
    let statement_from_id = |i: NodeIndex| {
        let mut x = ids.partition_point(|&j| j < i.index());
        if ids[x] > i.index() {
            x -= 1;
        }
        (
            rules[x].get_tupfile(),
            rules[x].get_tupfile_node(),
            rules[x].get_statement(i.index() - ids[x]),
        )
    };
    let mut db_group_provider: HashMap<i64, NodeIndex> = HashMap::new();

    let mut db_rules_that_provide: HashMap<NodeIndex, Node> = HashMap::new();
    let mut find_groupid = conn.fetch_groupid_prepare()?;
    let mut outputtags = OutputTagInfo::new();
    if false {
        for (group, _) in required_by.iter() {
            if let Some(g) = group.as_str().strip_prefix("?G") {
                if let Ok(groupid) = find_groupid.fetch_groupid(g) {
                    conn.for_each_grp_node_provider(groupid, |nodeid, dir, dir_path| {
                        let e = db_group_provider.entry(nodeid.clone());
                        e.or_insert_with(|| {
                            let dag_node_index = dag.add_node(1);
                            let mut v: Vec<NodeIndex> = Vec::new();
                            provided_by
                                .get_mut(group)
                                .get_or_insert(&mut v)
                                .push(dag_node_index);
                            db_rules_that_provide.insert(
                                dag_node_index,
                                Node::new(
                                    nodeid.clone(),
                                    dir,
                                    0,
                                    dir_path.to_string_lossy().to_string(),
                                    RuleType,
                                ),
                            );
                            // add grp providers from other tupfiles
                            outputtags
                                .groups
                                .entry(GroupPathDescriptor::new(groupid.clone() as usize))
                                .or_insert(std::collections::HashSet::new())
                                .extend(std::iter::once(PathDescriptor::new(nodeid as usize)));
                            dag_node_index
                        });
                        Ok(())
                    })?;
                }
            }
        }
    }
    let mut find_in_dir = conn.fetch_node_prepare()?;
    let mut dirs_in_db = conn.fetch_dirid_prepare()?;
    let mut inp_linker = conn.insert_sticky_link_prepare()?;
    let mut out_linker = conn.insert_link_prepare()?;

    let mut insert_link = |name: String, dirpath, rule_id, is_input: bool| -> Result<Node> {
        if let Some(dirid) = get_dir_id(&mut dirs_in_db, dirpath) {
            if let Ok(node) = find_in_dir.fetch_node(name.as_str(), dirid) {
                if is_input {
                    inp_linker.insert_sticky_link(node.get_id(), rule_id)?;
                } else {
                    out_linker.insert_link(rule_id, node.get_id())?;
                }
                Ok(node)
            }else {
                Err(anyhow::Error::msg("no node found"))
            }
        }else {
            Err(anyhow::Error::msg("no node found"))
        }
    };

    for (group, nodeids) in required_by.iter() {
        if let Some(pnodeids) = provided_by.get(group) {
            for pnodeid in pnodeids {
                for nodeid in nodeids {
                    dag.update_edge(*pnodeid, *nodeid, 1).map_err(|_| {
                        tupparser::errors::Error::DependencyCycle(
                            {
                                let (tupfile, _, stmt) = statement_from_id(*pnodeid);
                                format!(
                                    "tupfile:{}, and rule:{}",
                                    tupfile.to_string_lossy(),
                                    stmt.cat()
                                )
                            },
                            {
                                let (tupfile, _, stmt) = statement_from_id(*nodeid);
                                format!(
                                    "tupfile:{}, and rule:{}",
                                    tupfile.to_string_lossy(),
                                    stmt.cat()
                                )
                            },
                        )
                    })?;
                }
            }
        } else {
            println!("Group {} has no providers", group);
        }
    }
    // We dont yet have decoded paths and so cannot correctly specify
    // dependencies between rules in the same tupfile
    // To kickstart the decoding process, add dependencies within each tupfile between successive rules.
    // if this causes cyclic deps we ignore them.
    for slic in ids.as_slice().windows(2) {
        for nid in (slic[0] + 1)..slic[1] {
            if statement_from_id(NodeIndex::new(nid - 1)).0
                == statement_from_id(NodeIndex::new(nid)).0
            // same tupfile
            {
                let _res = dag.update_edge(NodeIndex::new(nid - 1), NodeIndex::new(nid), 1);
            }
        }
        // ignore cylic deps at this point, these are soft constraints..
    }
    let nodes: Vec<_> = daggy::petgraph::algo::toposort(&dag, None).map_err(|e| {
        tupparser::errors::Error::DependencyCycle("".to_string(), {
            let (tupfile, _, stmt) = statement_from_id(e.node_id());
            format!(
                "tupfile:{}, and rule:{}",
                tupfile.to_string_lossy(),
                stmt.cat()
            )
        })
    })?;

    let mut outputtags = OutputTagInfo::new();
    let mut lstats = Vec::new();
    let mut bo = BufferObjects::default();
    let mut insert_rule = conn.insert_node_prepare()?;
    for tupnodeid in nodes {
        let (tupfile,tupfile_node, statement) = statement_from_id(tupnodeid);
        let tupdesc = TupPathDescriptor::new(tupnodeid.index());
        let (resolved_links, ref mut newoutputtags) =
            statement.resolve_paths(tupfile, &outputtags, &mut bo, &tupdesc)?;
        let stmt_str = statement.getstatement().cat();
        let maybe_rule_id = db_rules
            .iter()
            .find(|e| e.get_name() == stmt_str)
            .map(|n| n.get_id());
        let newrule = maybe_rule_id.is_some();
        let ruleid = if let Some(id) = maybe_rule_id {
            id
        } else {
            insert_rule.insert_node_exec(&Node::new(
                0,
                tupfile_node.get_pid(),
                tupfile_node.get_mtime(),
                statement.getstatement().cat(),
                RuleType,
            ))?
        };

        if !newrule {
            // for an already existing rule,
            // it is messy to discover what changed among links joining this rule,
            // we will just wipe the slate clean and build links to/from this rule again
           // del_stmt.delete_rule_links(ruleid.clone())?;
        }
        for mut rl in resolved_links {
            for l in rl
                .primary_sources
                .drain(..)
                .chain(rl.secondary_sources.drain(..))
            {
                let fname = bo.get_name(&l);
                let dir = bo.get_dir(&l).to_path_buf();
                insert_link(fname, dir, ruleid.clone(), true)?;
            }
            for l in rl
                .primary_target
                .drain(..)
                .chain(rl.secondary_targets.drain(..))
            {
                let path: &Path = bo.get_path_buffer_object().get(&l).into();
                let fname = path.file_name().unwrap().to_string_lossy().to_string();
                let dir = path.parent().unwrap().to_path_buf();
                let  n = insert_link(fname, dir, ruleid.clone(), false)?;
                {
                    for l in rl.group.iter() {
                        let gbo = bo.get_group_buffer_object();
                        let path: &Path = gbo.get(l).into();
                        let fname = path.file_name().unwrap().to_string_lossy().to_string();
                        let dir = path.parent().unwrap().to_path_buf();
                        insert_link(fname, dir, n.get_id(), false)?;
                    }
                }
            }
            for l in rl.group.iter() {
                let gbo = bo.get_group_buffer_object();
                let path: &Path = gbo.get(l).into();
                let fname = path.file_name().unwrap().to_string_lossy().to_string();
                let dir = path.parent().unwrap().to_path_buf();
                insert_link(fname, dir, ruleid.clone(), false)?;
            }
            for l in rl.bin.iter() {
                let bbo = bo.get_bin_buffer_object();
                let path: &Path = bbo.get(l).into();
                let fname = path.file_name().unwrap().to_string_lossy().to_string();
                let dir = path.parent().unwrap().to_path_buf();
                insert_link(fname, dir, ruleid.clone(), false)?;
            }
            lstats.push(rl)
        }
        outputtags.merge_group_tags(newoutputtags)?;
        outputtags.merge_bin_tags(newoutputtags)?;
    }
    Ok(lstats)
}

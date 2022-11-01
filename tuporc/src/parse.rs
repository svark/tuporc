use crate::db::{create_dir_path_buf_temptable, create_group_path_buf_temptable, create_tup_path_buf_temptable, ForEachClauses, LibSqlExec, SqlStatement};
use crate::RowType::RuleType;
use crate::{get_dir_id, LibSqlPrepare, Node, RowType};
use anyhow::Result;
use bimap::BiMap;
use rusqlite::Connection;
use std::collections::HashMap;
use std::path::{Path};
use tupparser::decode::{
    BufferObjects, GroupPathDescriptor, InputResolvedType, OutputTagInfo, PathDescriptor,
     ResolvedLink, RuleDescriptor,
};
use tupparser::transform::{load_conf_vars, parse_tup};

pub struct ParsedStatements {
    tupfile_node: Node,
    statements: Vec<ResolvedLink>,
}
impl ParsedStatements {
    pub fn new(
        tupfile_node: Node,
        statements: Vec<ResolvedLink>,
    ) -> ParsedStatements {
        ParsedStatements {
            tupfile_node,
            statements,
        }
    }
    /*pub fn get_tupfile_node(&self) -> &Node {
        return &self.tupfile_node;
    }
    pub fn get_statement(&self, i: usize) -> &ResolvedLink {
        &self.statements[i]
    }
     */
    pub fn get_statements(&self) -> &[ResolvedLink] {
        self.statements.as_slice()
    }
}
/*
pub fn get_group_providers(
    conn: &Connection,
    groupid: i64,
    rtype: Option<RowType>,
) -> Result<Vec<i64>> {
    let mut results = Vec::new();
    conn.for_each_grp_nodeid_provider(groupid, rtype, |n| {
        results.push(n);
        Ok(())
    })?;
    Ok(results)
}
*/
// CrossRefMaps track database ids of paths, groups and rules using BiMap
#[derive(Debug, Clone, Default)]
pub struct CrossRefMaps {
    gbo: BiMap<GroupPathDescriptor, i64>,
    pbo: BiMap<PathDescriptor, i64>,
    rbo: BiMap<RuleDescriptor, i64>,
}

impl CrossRefMaps {
    pub fn get_group_db_id(&self, g: &GroupPathDescriptor) -> Option<i64> {
        self.gbo.get_by_left(g).map(|i| *i)
    }
    pub fn get_path_db_id(&self, p: &PathDescriptor) -> Option<i64> {
        self.pbo.get_by_left(p).map(|i| *i)
    }
    pub fn get_rule_db_id(&self, r: &RuleDescriptor) -> Option<i64> {
        self.rbo.get_by_left(r).map(|i| *i)
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

    let rootfolder = tupparser::parser::locate_file(root.as_ref(), "Tupfile.ini")
        .ok_or(tupparser::errors::Error::RootNotFound)?;
    let confvars = load_conf_vars(rootfolder.as_path())?;
    conn.for_each_tup_node_with_path(|n: Node| {
        // name stores full path here
        tupfiles.push(n);
        Ok(())
    })?;
    let mut bo = BufferObjects::default();
    let mut rules_in_tup_file = Vec::new();
    let mut new_outputs = OutputTagInfo::new_no_resolve_groups();
    {
        //let mut del_stmt = conn.delete_tup_rule_links_prepare()?;
        for tupfile_node in tupfiles.iter() {
            // try fetching statements in this tupfile already in the database to avoid inserting same rules again
            let tup_node_name = tupfile_node.get_name();
            let (rlinks, mut o, newbo) = parse_tup(&confvars, tup_node_name, bo)?;
            bo = newbo;
            new_outputs.merge(&mut o)?;
            //   let dir_id = tupfile_node.get_pid();
            // del_stmt.delete_rule_links(dir_id)?; // XTODO: Mark nodes as being deleted, track leaves left unreachable in the graph that depend on this node.
            //db_rules.extend(conn.fetch_db_rules(dir_id)?);
            // need to mark generated nodes from rules of this tupfile as deleted unless resurrected below
            rules_in_tup_file.push(ParsedStatements::new(
                tupfile_node.clone(),
                rlinks,
            ));
        }
    }

    let mut crossref = CrossRefMaps::default();
    insert_nodes(conn, &mut bo, &mut rules_in_tup_file, &mut crossref)?;

    {
        let mut parent_rule = conn.fetch_parent_rule_prepare()?;
        let mut fetch_rule = conn.fetch_node_by_id_prepare()?;
        for o in new_outputs.output_files {
            let db_id_of_o = crossref.get_path_db_id(&o).expect("path not found");
            if let Ok(rule_id) = parent_rule.fetch_parent_rule(db_id_of_o) {
                let node = fetch_rule.fetch_node_by_id(rule_id)?;
                let parent_rule_ref = new_outputs
                    .parent_rule
                    .get(&o)
                    .expect("unable to fetch parent rule");
                let tbo = bo.get_tup_buffer_object();
                let path = tbo.get(parent_rule_ref.get_dir_desc());
                return Err(anyhow::Error::msg(
                    format!("File was previously marked as generated from a rule:{} but is now being generated in Tupfile {} line:{}",
                            node.get_name(),  path.as_path().to_string_lossy().to_string(), parent_rule_ref.get_line()
                    )
));
            }
        }
    }
    {
        //XTODO: delete previous links from output files to groups
        add_links_to_groups(conn, &mut rules_in_tup_file, &crossref)?;
    }
    {
        // get a global list of providers of a each group
        for group_desc in bo.get_group_buffer_object().get_ids() {
            let groupid = crossref.get_group_db_id(group_desc).expect(&*format!(
                "could not fetch groupid from its internal id:{}",
                group_desc
            ));
            conn.for_each_grp_node_provider(groupid, None, |node| -> Result<()> {
                let nodeid = node.get_id();

                if *node.get_type() == RowType::GenFType {
                    // merge providers of this group from all available in db
                    new_outputs
                        .groups
                        .entry(*group_desc)
                        .or_default()
                        .extend(std::iter::once(PathDescriptor::new(nodeid as usize)));
                }

                Ok(())
            })?;
        }
    }

    {
        let tconn = conn.transaction()?;
        let mut inp_linker = tconn.insert_sticky_link_prepare()?;
        let mut out_linker = tconn.insert_link_prepare()?;
        for  r in rules_in_tup_file {
            for rl in r.statements {
                let rule_node_id = crossref
                    .get_rule_db_id(&rl.rule_formula_desc)
                    .expect("rule dbid fetch failed");
                for i in rl.primary_sources.iter().chain(rl.secondary_sources.iter()) {
                    let mut added: bool = false;
                    match i {
                        InputResolvedType::UnResolvedGroupEntry(g, _) => {
                            if let Some(group_id) = crossref.get_group_db_id(g) {
                                inp_linker.insert_sticky_link(group_id, rule_node_id)?;
                                added = true;
                            }
                        }
                        InputResolvedType::Deglob(mp) => {
                            if let Some(pid) = crossref.get_path_db_id(mp.path_descriptor()) {
                                inp_linker.insert_sticky_link(pid, rule_node_id)?;
                                added = true;
                            }
                        }
                        InputResolvedType::BinEntry(_, p) => {
                            if let Some(pid) = crossref.get_path_db_id(p) {
                                inp_linker.insert_sticky_link(pid, rule_node_id)?;
                                added = true;
                            }
                        }
                        InputResolvedType::GroupEntry(g, _) => {
                            if let Some(group_id) = crossref.get_group_db_id(g) {
                                inp_linker.insert_sticky_link(group_id, rule_node_id)?;
                                added = true;
                            }
                        }
                    }
                    if !added {
                        let fname = bo.get_name(i);
                        let dir = bo.get_dir(i).to_path_buf();

                        anyhow::ensure!(
                            false,
                            format!(
                                "could not add a link from input {:?}/{} to ruleid:{}",
                                dir.as_path(),
                                fname,
                                rule_node_id
                            )
                        );
                    }
                }
                {
                    for i in rl.primary_targets.iter().chain(rl.secondary_targets.iter()) {
                        let p = crossref
                            .get_path_db_id(i)
                            .expect(&*format!("failed to fetch db id of path {}", i));
                        out_linker.insert_link(rule_node_id, p)?;
                    }
                }
            }
        }
    }
    Ok(Vec::new())
}

fn insert_nodes(
    conn: &mut Connection,
    bo: &mut BufferObjects,
    rules_in_tup_file: &mut Vec<ParsedStatements>,
    crossref: &mut CrossRefMaps,
) -> Result<()> {
    let mut groups_to_insert: Vec<_> = Vec::new();
    let mut paths_to_insert = Vec::new();
    let mut rules_to_insert = Vec::new();
    let mut paths_to_update: HashMap<i64, i64> = HashMap::new();
    // collect all un-added groups and add them in a single transaction.
    {
        let mut find_dirid = conn.fetch_dirid_prepare()?;
        let mut find_group_id = conn.fetch_groupid_prepare()?;
        for group_path in bo.get_group_buffer_object().get_paths() {
            let parent = group_path.as_path().parent().unwrap();
            if let Some(dir) = get_dir_id(&mut find_dirid,
                parent.to_string_lossy().to_string())
            {
                let id = find_group_id.fetch_group_id(group_path.as_path()).ok();
                let grp_id = *bo
                    .get_group_buffer_object()
                    .try_get_id(group_path)
                    .expect("we expect group id to be already in  buffer");
                if let Some(i) = id {
                    // grp_db_id.insert(grp_id, i);
                    crossref.add_group_xref(grp_id, i);
                } else {
                    // gather groups that are not in the db yet.
                    let isz: usize = grp_id.into();
                    groups_to_insert.push(Node::new(
                        isz as i64,
                        dir,
                        0,
                        group_path.as_path().to_string_lossy().to_string(),
                        RowType::GrpType,
                    ));
                }
            }
        }
        let mut find_nodeid = conn.fetch_nodeid_prepare()?;

        let mut collect_rule_nodes_to_insert = |rule_desc: &RuleDescriptor, dir: i64, crossref: &mut CrossRefMaps, find_nodeid: &mut SqlStatement| {
            let typ = RuleType;
            let isz: usize = (*rule_desc).into();
            let rformula = bo.get_rule(rule_desc);
            let name = format!("{}", rformula);
            if let Ok(nodeid) = find_nodeid.fetch_node_id(name.as_str(), dir) {
                crossref.add_rule_xref(*rule_desc, nodeid);
            } else {
                rules_to_insert.push(Node::new(isz as i64, dir, 0, name, typ));
            }
        };
        let mut collect_nodes_to_insert =
            |p: &PathDescriptor, typ: &RowType, mtime_ns: i64, crossref: &mut CrossRefMaps, find_nodeid: &mut SqlStatement| -> Result<()> {
                let isz: usize = (*p).into();
                let path = bo.get_path_buffer_object().get(p);
                let parent = path.as_path().parent().expect(&*format!(
                    "No parent folder found for file {:?}",
                    path.as_path()
                ));
                let dir = find_dirid.fetch_dirid(parent)?;
                let name = path
                    .as_path()
                    .file_name()
                    .map(|s| s.to_string_lossy().to_string());
                if let Ok(nodeid) = find_nodeid.fetch_node_id(
                    &name.expect(&*format!("missing name:{:?}", path.as_path())),
                    dir,
                ) {
                    //path_db_id.insert(p, nodeid);
                    crossref.add_path_xref(*p, nodeid);
                    paths_to_update.insert(nodeid, mtime_ns);
                } else {
                    paths_to_insert.push(Node::new(
                        isz as i64,
                        dir,
                        mtime_ns,
                        path.as_path().to_string_lossy().to_string(),
                        *typ,
                    ));
                }
                Ok(())
            };
        for r in rules_in_tup_file {
            let dir = r.tupfile_node.get_id();
            let mtime = r.tupfile_node.get_mtime();
            //   let db_rules = conn.fetch_db_rules(dir)?;

            for rl in r.statements.iter() {
                let rd = rl.get_rule_desc();
                collect_rule_nodes_to_insert(rd, dir, crossref, &mut find_nodeid);
                for p in rl.primary_targets.iter().chain(rl.secondary_targets.iter()) {
                    collect_nodes_to_insert(p, &RowType::GenFType, mtime, crossref, &mut find_nodeid)?;
                }

                for i in rl.primary_sources.iter().chain(rl.secondary_sources.iter()) {
                    match i {
                        InputResolvedType::Deglob(mp) => {
                            collect_nodes_to_insert(mp.path_descriptor(), &RowType::FileType, mtime, crossref, &mut find_nodeid)?;
                        }
                        _ => {}
                    };
                }
            }
        }
    }
    {
        let tx = conn.transaction()?;
        let mut insert_node = tx.insert_node_prepare()?;
        for node in groups_to_insert
            .into_iter()
            .chain(paths_to_insert.into_iter())
            .chain(rules_to_insert.into_iter())
        {
            let desc = node.get_id() as usize;
            let db_id = insert_node.insert_node_exec(&node)?;
            if RowType::GrpType.eq(node.get_type()) {
                crossref.add_group_xref(GroupPathDescriptor::new(desc), db_id);
            } else if RuleType.eq(node.get_type()) {
                crossref.add_rule_xref(RuleDescriptor::new(desc), db_id);
            } else {
                crossref.add_path_xref(PathDescriptor::new(desc), db_id);
            }
        }
        //tx.commit()?;
    }
    Ok(())
}

fn add_links_to_groups(
    conn: &mut Connection,
    rules_in_tup_file: &Vec<ParsedStatements>,
    crossref: &CrossRefMaps,
) -> Result<()> {
    let tconn = conn.transaction()?;
    let mut inp_linker = tconn.insert_sticky_link_prepare()?;

    for r in rules_in_tup_file {
        for rl in r.get_statements().iter() {
            if let Some(group_id) = rl.group.as_ref() {
                if let Some(group_db_id) = crossref.get_group_db_id(&group_id) {
                    for target in rl.primary_targets.iter().chain(rl.secondary_targets.iter()) {
                        if let Some(path_db_id) = crossref.get_path_db_id(target) {
                            inp_linker.insert_link(path_db_id, group_db_id)?;
                        }
                    }
                }
            }
            for i in rl.primary_sources.iter().chain(rl.secondary_sources.iter()) {
                match i {
                    InputResolvedType::GroupEntry(g, p) => {
                        if let Some(group_id) = crossref.get_group_db_id(g) {
                            if let Some(pid) = crossref.get_path_db_id(p) {
                                inp_linker.insert_link(pid, group_id)?;
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(())
}

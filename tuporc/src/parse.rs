use crate::db::RowType::{GrpType, TupFType};
use crate::db::{
    create_group_pbuf_temptable, create_tup_pbuf_temptable, ForEachClauses, LibSqlExec, RowType,
};
use crate::RowType::RuleType;
use crate::{get_dir_id, is_tupfile, query_with_stmt, LibSqlPrepare, Node, SqlStatement};
use anyhow::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use rusqlite::Connection;
use tupparser::decode::{BufferObjects, InputResolvedType, OutputTagInfo, ResolvePaths, ResolvedLink, TupPathDescriptor, NormalPath};
use tupparser::statements::Cat;
use tupparser::statements::Statement::Rule;
use tupparser::transform::{load_conf_vars, parse_tup, Deps, ParsedStatements};

pub fn parse_tupfiles_in_db(conn: &rusqlite::Connection, root: &Path) -> Result<Vec<ResolvedLink>> {
    let mut tupfiles = Vec::new();
    create_group_pbuf_temptable(conn)?;
    create_tup_pbuf_temptable(conn)?;
    create_tup_outputs(conn)?;

    conn.for_each_tupnode_with_path(|i: i64, dirid: i64, entry: &Path| {
        if is_tupfile(entry.file_name().unwrap()) {
            let tupfilepath = entry.as_path().to_string_lossy().as_ref().to_string();
            tupfiles.push(Node::new(i, dirid, 0, tupfilepath, TupFType));
        }
        Ok(())
    })?;

    let rootfolder =
        locate_file(root, "Tupfile.ini").ok_or(tupparser::errors::Error::RootNotFound)?;
    let confvars = load_conf_vars(rootfolder.as_path())?;
    let mut bo = BufferObjects::default();
    let mut dirs_in_db = conn.find_dirid_prepare()?;
    let mut find_in_dir = conn.fetch_node_prepare()?;
    let mut insert_rule = conn.insert_node_prepare()?;
    let mut inp_linker = conn.insert_sticky_link_prepare()?;
    let mut out_linker = conn.insert_link_prepare()?;
   let insert_link = |path:&Path, rule_id: i64| -> Result<()> {
                let fname = path.file_name().unwrap().to_string_lossy().to_string();
                let dir = path.parent().unwrap();
                if let Some(dirid) = get_dir_id(&mut dirs_in_db, dir) {
                    if let Ok(node) = find_in_dir.fetch_name(dirid, fname) {
                        if !output_links.contains(&(node.get_id(), tupfile_node.get_id())) {
                            out_linker.insert_link( rule_id,node.get_id())?;
                        }
                    }
                }
                Ok(())
            };
    for tupfile_node in tupfiles.iter() {
        // try fetching statements in this tupfile already in the database to avoid inserting same rules again
        let db_stmts = conn.fetch_db_rules(tupfile_node.get_pid())?;
        let mut input_links = HashSet::new();
        let mut input_sticky_links = HashSet::new();
        let mut output_links = HashSet::new();
        fetch_links_in_db_for_stmt(conn, tupfile_node, &db_stmts, &mut input_links, &mut input_sticky_links, &mut output_links);
        let stmts = parse_tup(&confvars, tupfilepath.as_str())?;
        let tupdesc = TupPathDescriptor::new(tupfile_node.get_id() as usize);
        for statement in stmts {
            let (resolved_links, ref mut newoutputtags) =
                statement.resolve_paths(tupfile, &outputtags, &mut bo, &tupdesc)?;

            let (ruleid,newrule) = insert_rule.insert_node_exec(Node::new(
                0,
                tupfile_node.get_pid(),
                tupfile_node.get_mtime(),
                statement.getstatement().cat(),
                RuleType,
            ), &db_stmts[..])?;



            for mut rl in resolved_links {
                rl.primary_sources.drain(..).chain(rl.secondary_sources.drain(..))
                    .for_each(|l| {
                    if let Some(dirid) = get_dir_id(&mut dirs_in_db, bo.get_dir(&l)) {
                        if let Ok(node) = find_in_dir.fetch_node(dirid, bo.get_name(&l)) {
                            if !input_sticky_links.contains(&(node.get_id(), tupfile_node.get_id())) {
                                inp_linker.insert_sticky_link(node.get_id(), ruleid)?;
                            }
                        }
                    }
                });
                rl.primary_target.drain(..).chain(rl.secondary_targets.drain(..)).
                    for_each(|l|
                    {
                        let normal_path = bo.get_path_buffer_object().get(&l);
                        insert_link(normal_path.into(), ruleid)?;
                    });
                rl.group.iter().for_each(|l| {
                    let gbo = bo.get_group_buffer_object();
                    let normal_path = gbo.get(l);
                    insert_link(normal_path.into(), ruleid)?;
                });
                rl.bin.iter().for_each(|l| {
                    let bbo = bo.get_bin_buffer_object();
                    let normal_path = bbo.get(l);
                    insert_link(normal_path.into(), ruleid)?;
                })
            }
        }
    }
    let mut outputtags = OutputTagInfo::new();
    let mut lstats = Vec::new();
    for tupnodeid in nodes {
        let (tupfile, statement) = statement_from_id(tupnodeid);
        outputtags.merge_group_tags(newoutputtags)?;
        outputtags.merge_bin_tags(newoutputtags)?;
        lstats.extend(resolved_links.into_iter());
    }
    Ok(lstats)
}

fn fetch_links_in_db_for_stmt(conn: &Connection, tupfile_node: &Node, db_stmts: &Vec<Node>, input_links: &mut HashSet<(i64, i64)>, input_sticky_links: &mut HashSet<(i64, i64)>, output_links: &mut HashSet<(i64, i64)>) {
    for node in db_stmts {
        let inps = conn.fetch_db_sticky_inputs(node.get_id())?;
        for inp in inps {
            input_sticky_links.insert((inp, tupfile_node.get_id()));
        }
        let inps = conn.fetch_db_sticky_inputs()?;
        for inp in inps {
            input_links.insert((inp, tupfile_node.get_id()));
        }
        let outs = conn.fetch_db_outputs(node.get_id())?;
        for out in outs {
            output_links.insert((out, tupfile_node.get_id()));
        }
    }
}


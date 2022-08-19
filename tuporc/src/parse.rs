use crate::db::RowType::TupFType;
use crate::db::{
    create_dir_path_buf_temptable, create_group_path_buf_temptable, create_tup_path_buf_temptable,
    ForEachClauses, LibSqlExec,
};
use crate::RowType::RuleType;
use crate::{get_dir_id, is_tupfile, LibSqlPrepare, Node};
use anyhow::Result;
use rusqlite::Connection;
use std::path::Path;
use tupparser::decode::{
    BufferObjects, OutputTagInfo, ResolvePaths, ResolvedLink, TupPathDescriptor,
};
use tupparser::statements::Cat;
use tupparser::transform::{load_conf_vars, parse_tup};

// handle the tup parse command which assumes files in db and adds rules and makes links joining input and output to/from rule statements
pub fn parse_tupfiles_in_db<P:AsRef<Path>>(conn: &Connection, root: P) -> Result<Vec<ResolvedLink>> {
    let mut tupfiles = Vec::new();
    create_dir_path_buf_temptable(conn)?;
    create_group_path_buf_temptable(conn)?;
    create_tup_path_buf_temptable(conn)?;
    //create_tup_outputs(conn)?;

    conn.for_each_tup_node_with_path(|i: i64, dirid: i64, entry: &Path| {
        if is_tupfile(entry.file_name().unwrap()) {
            let tupfilepath = entry.to_string_lossy().as_ref().to_string();
            tupfiles.push(Node::new(i, dirid, 0, tupfilepath, TupFType));
        }
        Ok(())
    })?;

    let rootfolder = tupparser::parser::locate_file(root.as_ref(), "Tupfile.ini")
        .ok_or(tupparser::errors::Error::RootNotFound)?;
    let confvars = load_conf_vars(rootfolder.as_path())?;
    let mut bo = BufferObjects::default();
    let mut dirs_in_db = conn.find_dirid_prepare()?;
    let mut find_in_dir = conn.fetch_node_prepare()?;
    let mut insert_rule = conn.insert_node_prepare()?;
    let mut inp_linker = conn.insert_sticky_link_prepare()?;
    let mut out_linker = conn.insert_link_prepare()?;
    let mut insert_link = |name: String, dirpath, rule_id, sticky: bool| -> Result<()> {
        if let Some(dirid) = get_dir_id(&mut dirs_in_db, dirpath) {
            if let Ok(node) = find_in_dir.fetch_node(name.as_str(), dirid) {
                if sticky {
                    inp_linker.insert_sticky_link(node.get_id(), rule_id)?;
                } else {
                    out_linker.insert_link(rule_id, node.get_id())?;
                }
            }
        }
        Ok(())
    };

    let mut outputtags = OutputTagInfo::new();
    let lstats = Vec::new();
    let mut del_stmt = conn.delete_rule_links_prepare()?;
    for tupfile_node in tupfiles.iter() {
        // try fetching statements in this tupfile already in the database to avoid inserting same rules again
        let db_stmts = conn.fetch_db_rules(tupfile_node.get_pid())?;
        let tupfilepath = tupfile_node.get_name();
        let stmts = parse_tup(&confvars, tupfilepath)?;
        let tupdesc = TupPathDescriptor::new(tupfile_node.get_id() as usize);
        outputtags.clearbins();
        for statement in stmts {
            let (resolved_links, ref mut newoutputtags) = {
                statement.resolve_paths(Path::new(tupfilepath), &outputtags, &mut bo, &tupdesc)?
            };

            let stmt_str = statement.getstatement().cat();
            let maybe_rule_id = db_stmts.iter().find( |e| e.get_name() == stmt_str).map(|n| n.get_id());
            let newrule = maybe_rule_id.is_some();
            let ruleid =
                if let Some(id) = maybe_rule_id {
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
                del_stmt.delete_rule_links(ruleid)?;
            }

            for mut rl in resolved_links {
                for l in rl
                    .primary_sources
                    .drain(..)
                    .chain(rl.secondary_sources.drain(..))
                {
                    let fname = bo.get_name(&l);
                    let dir = bo.get_dir(&l).to_path_buf();
                    insert_link(fname, dir, ruleid, true)?;
                }
                for l in rl
                    .primary_target
                    .drain(..)
                    .chain(rl.secondary_targets.drain(..))
                {
                    let path: &Path = bo.get_path_buffer_object().get(&l).into();
                    let fname = path.file_name().unwrap().to_string_lossy().to_string();
                    let dir = path.parent().unwrap().to_path_buf();
                    insert_link(fname, dir, ruleid, false)?;
                }
                for l in rl.group.iter() {
                    let gbo = bo.get_group_buffer_object();
                    let path: &Path = gbo.get(l).into();
                    let fname = path.file_name().unwrap().to_string_lossy().to_string();
                    let dir = path.parent().unwrap().to_path_buf();
                    insert_link(fname, dir, ruleid, false)?;
                }
                for l in rl.bin.iter() {
                    let bbo = bo.get_bin_buffer_object();
                    let path: &Path = bbo.get(l).into();
                    let fname = path.file_name().unwrap().to_string_lossy().to_string();
                    let dir = path.parent().unwrap().to_path_buf();
                    insert_link(fname, dir, ruleid, false)?;
                }
            }
            outputtags.merge_bin_tags(newoutputtags)?;
        }
    }
    Ok(lstats)
}


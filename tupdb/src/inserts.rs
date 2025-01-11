use crate::db::{Node, RowType};
use crate::error::{AnyError, DbResult};
use crate::queries::LibSqlQueries;
use include_sqlite_sql::{impl_sql, include_sql};
use rusqlite::{Connection, Result};

include_sql!("src/sql/inserts.sql");

pub trait LibSqlInserts {
    fn insert_node(&self, n: &Node) -> DbResult<i64>;
    fn upsert_env_var(&self, name: &str, value: &str) -> DbResult<UpsertStatus>;
    fn update_columns(
        &self,
        node: &Node,
        existing_node: &Node,
        compute_sha256: impl FnMut() -> String,
    ) -> Result<(), AnyError>;
    fn upsert_node(&self, node: &Node, f: impl FnMut() -> String) -> Result<Node, AnyError>;
    fn mark_modified(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn mark_rule_succeeded(&self, rule_id: i64) -> Result<()>;
    fn mark_present(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn mark_deleted(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn update_mtime_exec(&self, nodeid: i64, mtime: i64) -> DbResult<usize>;
    fn update_type_exec(&self, nodeid: i64, row_type: RowType) -> DbResult<usize>;
    fn update_display_str_exec(&self, nodeid: i64, display_str: &str) -> DbResult<usize>;
    fn update_flags_exec(&self, nodeid: i64, flags: &str) -> DbResult<usize>;
    fn update_srcid_exec(&self, nodeid: i64, srcid: i64) -> DbResult<usize>;
    fn update_node_sha_exec(&self, nodeid: i64, sha: &str) -> DbResult<usize>;
    fn insert_link(&self, src: i64, dst: i64, is_sticky: u8, dst_type: RowType) -> DbResult<usize>;
    fn insert_trace(
        &self,
        path: &str,
        pid: i64,
        gen: i64,
        typ: u8,
        childcnt: i64,
    ) -> DbResult<usize>;
    fn mark_tupfile_entries(&self, tupfile_ids: &Vec<i64>) -> DbResult<()>;
    fn create_tupfile_entries_table(&self) -> DbResult<()>;
    fn drop_tupfile_entries_table(&self) -> DbResult<()>;
    fn add_not_present_to_delete_list(&self) -> DbResult<()>;
    fn add_rules_with_changed_io_to_modify_list(&self) -> DbResult<()>;
    fn mark_dependent_tupfiles_groups(&self) -> DbResult<()>;
    fn mark_dependent_tupfiles_of_glob(&self, glob_id: i64) -> DbResult<()>;
    fn delete_tupfile_entries_not_in_present_list(&self) -> DbResult<()>;
    fn delete_tupentries_in_deleted_tupfiles(&self) -> DbResult<()>;
    fn upsert_node_sha(&self, node_id: i64, sha: &str) -> DbResult<UpsertStatus>;
    fn insert_monitored(&self, path: &str, gen_id: i64, event: i32) -> DbResult<usize>;
}

pub enum UpsertStatus {
    Inserted(i64),
    Updated(i64),
    Unchanged(i64),
}
impl UpsertStatus {
    pub fn get_id(&self) -> i64 {
        match self {
            UpsertStatus::Inserted(id) => *id,
            UpsertStatus::Updated(id) => *id,
            UpsertStatus::Unchanged(id) => *id,
        }
    }
    pub fn is_unchanged(&self) -> bool {
        match self {
            UpsertStatus::Unchanged(_) => true,
            _ => false,
        }
    }
}

impl LibSqlInserts for Connection {
    fn insert_node(&self, n: &Node) -> DbResult<i64> {
        let r = self.insert_node_inner(
            n.get_dir(),
            n.get_name(),
            n.get_mtime(),
            *n.get_type() as u8,
            n.get_display_str(),
            n.get_flags(),
            n.get_srcid(),
            |row| -> Result<i64> { row.get(0) },
        )?;
        Ok(r)
    }
    fn upsert_env_var(&self, name: &str, value: &str) -> DbResult<UpsertStatus> {
        if let Some((id, val)) = self.fetch_env(name).ok() {
            if val == value {
                return Ok(UpsertStatus::Unchanged(id));
            }
            self.update_env_var(value, id)?;
            return Ok(UpsertStatus::Updated(id));
        }
        let new_id = self.insert_env_var_inner(name, value, |row| -> Result<i64> { row.get(0) })?;
        Ok(UpsertStatus::Inserted(new_id))
    }
    fn update_columns(
        &self,
        node: &Node,
        existing_node: &Node,
        mut compute_sha256: impl FnMut() -> String,
    ) -> Result<(), AnyError> {
        let mut modify = false;
        // verify that columns of this row are the same as `node`'s, if not update them
        if existing_node.get_type().ne(&node.get_type()) {
            if existing_node.is_generated() {
                log::debug!(
                    "Keeping row type of generated file: {} until no rule writes to it",
                    existing_node.get_name()
                );
            } else {
                log::debug!(
                    "update type for:{}, {} -> {}",
                    existing_node.get_name(),
                    existing_node.get_type(),
                    node.get_type()
                );
                //node_statements.update_type(existing_node.get_id(), *node.get_type())?;
                self.update_type(*node.get_type() as u8, existing_node.get_id())?;
                // modify = true;
            }
        }
        if (existing_node.get_mtime() - node.get_mtime()).abs() > 1 {
            log::debug!(
                "updating mtime for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_mtime(),
                node.get_mtime()
            );
            self.update_mtime_ns(node.get_mtime(), existing_node.get_id())?;
            modify = true;
            let sha256 = compute_sha256();
            if self
                .upsert_node_sha(existing_node.get_id(), sha256.as_str())?
                .is_unchanged()
            {
                modify = false;
            }
            //if compute_sha256
        }
        if existing_node.get_display_str() != node.get_display_str() {
            log::debug!(
                "updating display_str for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_display_str(),
                node.get_display_str()
            );
            //node_statements.update_display_str_exec(existing_node.get_id(), node.get_display_str())?;
            self.update_node_display_str(node.get_display_str(), existing_node.get_id())?;
            modify = true;
        }
        if existing_node.get_flags() != node.get_flags() {
            log::debug!(
                "updating flags for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_flags(),
                node.get_flags()
            );
            self.update_node_flags(node.get_flags(), existing_node.get_id())?;
            //node_statements.update_flags_exec(existing_node.get_id(), node.get_flags())?;
            modify = true;
        }
        if existing_node.get_srcid() != node.get_srcid() {
            log::debug!(
                "updating srcid for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_srcid(),
                node.get_srcid()
            );
            //node_statements.update_srcid_exec(existing_node.get_id(), node.get_srcid())?;
            self.update_node_srcid(node.get_srcid(), existing_node.get_id())?;
            modify = true;
        }
        if modify {
            self.mark_modified(existing_node.get_id(), existing_node.get_type())?;
        }
        self.mark_present(existing_node.get_id(), existing_node.get_type())?;
        Ok(())
    }

    /// [upsert_node] is akin to the sqlite upsert operation
    /// for existing nodes it updates the node's columns, marking the node to the modify list/present list in this process.
    /// for new nodes it adds the node to Node table and marks the node in modify list/present list tables
    fn upsert_node(&self, node: &Node, f: impl FnMut() -> String) -> Result<Node, AnyError> {
        let db_node = self
            .fetch_node_by_dir_and_name(node.get_dir(), node.get_name())
            .map_err(|e| e.into())
            .and_then(|existing_node| {
                self.update_columns(node, &existing_node, f)
                    .map(|_| existing_node)
            });
        match db_node {
            Ok(db_node) => Ok(db_node),
            Err(e) if e.has_no_rows() => {
                let node = self.insert_node(node).map(|i| Node::copy_from(i, node))?;

                self.mark_modified(node.get_id(), node.get_type())?;
                self.mark_present(node.get_id(), node.get_type())?;
                // node sha is not computed unless needed for a rule
                Ok::<Node, AnyError>(node)
            }
            Err(e) => Err::<Node, AnyError>(e.clone()),
        }
    }

    fn mark_modified(&self, id: i64, rtype: &RowType) -> Result<()> {
        self.add_to_modify_list(id, *rtype as u8)?;
        Ok(())
    }
    fn mark_rule_succeeded(&self, rule_id: i64) -> Result<()> {
        self.insert_or_ignore_into_success_list(rule_id)?;
        Ok(())
    }
    fn mark_present(&self, id: i64, rtype: &RowType) -> Result<()> {
        self.add_to_present_list(id, *rtype as u8)?;
        Ok(())
    }
    fn mark_deleted(&self, id: i64, rtype: &RowType) -> Result<()> {
        self.add_to_delete_list(id, *rtype as u8)?;
        Ok(())
    }

    fn update_mtime_exec(&self, nodeid: i64, mtime: i64) -> DbResult<usize> {
        self.update_mtime_ns(nodeid, mtime).map_err(|e| e.into())
    }

    fn update_type_exec(&self, nodeid: i64, row_type: RowType) -> DbResult<usize> {
        self.update_type(row_type as u8, nodeid)
            .map_err(|e| e.into())
    }

    fn update_display_str_exec(&self, nodeid: i64, display_str: &str) -> DbResult<usize> {
        self.update_node_display_str(display_str, nodeid)
            .map_err(|e| e.into())
    }
    fn update_flags_exec(&self, nodeid: i64, flags: &str) -> DbResult<usize> {
        self.update_node_flags(flags, nodeid).map_err(|e| e.into())
    }
    fn update_srcid_exec(&self, nodeid: i64, srcid: i64) -> DbResult<usize> {
        self.update_node_srcid(nodeid, srcid).map_err(|e| e.into())
    }
    fn update_node_sha_exec(&self, nodeid: i64, sha: &str) -> DbResult<usize> {
        self.insert_or_replace_node_sha(nodeid, sha)
            .map_err(|e| e.into())
    }
    fn insert_link(&self, src: i64, dst: i64, is_sticky: u8, dst_type: RowType) -> DbResult<usize> {
        self.insert_link_inner(src, dst, is_sticky, dst_type as u8)
            .map_err(|e| e.into())
    }

    fn insert_trace(
        &self,
        path: &str,
        pid: i64,
        gen: i64,
        typ: u8,
        childcnt: i64,
    ) -> DbResult<usize> {
        self.insert_trace_inner(path, pid, gen, typ, childcnt)
            .map_err(|e| e.into())
    }
    fn mark_tupfile_entries(&self, tupfile_ids: &Vec<i64>) -> DbResult<()> {
        self.add_rules_and_outputs_of_tupfile_entities(tupfile_ids.as_slice())?;
        Ok(())
    }

    fn create_tupfile_entries_table(&self) -> DbResult<()> {
        self.create_tupfile_entities_table_inner()?;
        Ok(())
    }
    fn drop_tupfile_entries_table(&self) -> DbResult<()> {
        self.drop_tupfile_entities_table_inner()?;
        Ok(())
    }

    fn add_not_present_to_delete_list(&self) -> DbResult<()> {
        self.add_not_present_to_delete_list_inner()?;
        Ok(())
    }
    fn add_rules_with_changed_io_to_modify_list(&self) -> DbResult<()> {
        self.add_rules_with_changed_io_to_modify_list_inner()?;
        Ok(())
    }
    fn mark_dependent_tupfiles_groups(&self) -> DbResult<()> {
        self.mark_rules_depending_on_modified_groups_inner()?;
        Ok(())
    }
    fn mark_dependent_tupfiles_of_glob(&self, glob_id: i64) -> DbResult<()> {
        self.mark_dependent_tupfiles_of_glob_inner(glob_id)?;
        Ok(())
    }
    fn delete_tupfile_entries_not_in_present_list(&self) -> DbResult<()> {
        self.delete_tupfile_entries_not_in_present_list_inner()?;
        Ok(())
    }

    fn delete_tupentries_in_deleted_tupfiles(&self) -> DbResult<()> {
        self.delete_tupentries_in_deleted_tupfiles_inner()?;
        Ok(())
    }

    fn upsert_node_sha(&self, node_id: i64, sha: &str) -> DbResult<UpsertStatus> {
        if self
            .fetch_saved_nodesha256(node_id)
            .map_or(true, |old_sha| sha.ne(&old_sha))
        {
            self.update_node_sha_exec(node_id, sha)?;
            Ok(UpsertStatus::Inserted(node_id))
        } else {
            Ok(UpsertStatus::Unchanged(node_id))
        }
    }
    fn insert_monitored(&self, path: &str, gen_id: i64, event: i32) -> DbResult<usize> {
        self.insert_monitored_inner(path, gen_id, event).map_err(|e| e.into())
    }
}

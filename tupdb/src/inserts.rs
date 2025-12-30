use crate::db::{Node, RowType};
use crate::error::{AnyError, DbResult};
use crate::queries::LibSqlQueries;
use rusqlite::{Connection, Result};
use tupdb_sql_macro::generate_prepared_statements;

/*
Format for sql statements:
-- name: <function_name> [!?->]
-- # Parameters:
-- param: param_name1: type1
-- param: param_name2: type2
<sql statements>
...
! -> return type is () is used for insert/update/delete statements
? -> takes a closure as a parameter to process each row returned by the query
-> -> insert returns the id of the inserted row
*/

generate_prepared_statements!("sql/inserts.sql");
pub trait LibSqlInserts {
    fn insert_node(&self, n: &Node) -> DbResult<i64>;
    fn upsert_env_var(&self, name: &str, value: &str) -> DbResult<UpsertStatus>;
    fn update_columns(
        &self,
        node: &Node,
        existing_node: &Node,
        compute_sha256: impl FnMut() -> String,
    ) -> Result<(), AnyError>;
    fn upsert_node(
        &self,
        node: &Node,
        existing_node: &Node,
        f: impl FnMut() -> String,
    ) -> Result<Node, AnyError>;
    fn fetch_upsert_node(
        &self,
        node: &Node,
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError>;
    /// Same as fetch_upsert_node but does not hide delete-marked rows (used during scan).
    fn fetch_upsert_node_raw(
        &self,
        node: &Node,
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError>;
    fn mark_modified(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn mark_rule_succeeded(&self, rule_id: i64) -> Result<()>;
    fn mark_present(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn mark_deleted(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn update_mtime_exec(&self, nodeid: i64, mtime: i64) -> DbResult<()>;
    fn update_type_exec(&self, nodeid: i64, row_type: RowType) -> DbResult<()>;
    fn update_display_str_exec(&self, nodeid: i64, display_str: &str) -> DbResult<()>;
    fn update_flags_exec(&self, nodeid: i64, flags: &str) -> DbResult<()>;
    fn update_srcid_exec(&self, nodeid: i64, srcid: i64) -> DbResult<()>;
    fn update_node_sha_exec(&self, nodeid: i64, sha: &str) -> DbResult<()>;
    fn insert_link(&self, src: i64, dst: i64, is_sticky: u8, dst_type: RowType) -> DbResult<()>;
    /// Insert or validate a unique Output -> Producer link (Producer can be Rule or Task).
    /// Enforces that a given output node has at most one producer globally (Rule or Task).
    /// If an existing different producer is found, returns an error.
    fn upsert_output_producer_link(
        &self,
        output_id: i64,
        producer_id: i64,
        producer_type: RowType,
    ) -> DbResult<()>;
    fn insert_trace(&self, path: &str, pid: i64, gen: i64, typ: u8, childcnt: i64) -> DbResult<()>;
    fn mark_tupfile_entries(&self, tupfile_ids: &Vec<i64>) -> DbResult<()>;
    fn create_tupfile_entries_table(&self) -> DbResult<()>;
    fn add_not_present_to_delete_list(&self) -> DbResult<()>;
    fn add_rules_with_changed_io_to_modify_list(&self) -> DbResult<()>;
    fn mark_rules_depending_on_modified_groups(&self) -> DbResult<()>;
    fn mark_dependent_tupfiles_of_tupfiles(&self) -> DbResult<()>;
    fn mark_dependent_tupfiles_of_glob(&self, glob_id: i64) -> DbResult<()>;
    fn replace_node_sha(&self, node_id: i64, sha: impl FnMut() -> String) -> DbResult<UpsertStatus>;
    fn upsert_node_sha(&self, node_id: i64, sha: &str) -> DbResult<UpsertStatus>;
    fn insert_monitored(&self, path: &str, gen_id: i64, event: i32) -> DbResult<()>;
    fn insert_into_dirpathuf(&self, id: i64, dir: i64, name: &str) -> DbResult<()>;
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
        let new_id = self.insert_env_var_inner(name, value)?;
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

    /// `upsert_node` is akin to the sqlite upsert operation
    /// for existing nodes it updates the node's columns, marking the node to the modify list/present list in this process.
    /// for new nodes it adds the node to Node table and marks the node in modify list/present list tables
    fn upsert_node(
        &self,
        node: &Node,
        existing_node: &Node,
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError> {
        if existing_node.is_valid() {
            self.update_columns(node, &existing_node, compute_sha)?;
            Ok(Node::copy_from(existing_node.get_id(), node))
        } else {
            let node = self.insert_node(node).map(|i| Node::copy_from(i, node))?;
            self.mark_modified(node.get_id(), node.get_type())?;
            self.mark_present(node.get_id(), node.get_type())?;
            // node sha is not computed unless needed for a rule
            Ok::<Node, AnyError>(node)
        }
    }
    fn fetch_upsert_node(
        &self,
        node: &Node,
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError> {
        let existing_node = self
            .fetch_node_by_dir_and_name(node.get_dir(), node.get_name())
            .unwrap_or(Node::unknown());
        self.upsert_node(node, &existing_node, compute_sha)
    }
    fn fetch_upsert_node_raw(
        &self,
        node: &Node,
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError> {
        let existing_node = self
            .fetch_node_by_dir_and_name_raw(node.get_dir(), node.get_name())
            .unwrap_or(Node::unknown());
        self.upsert_node(node, &existing_node, compute_sha)
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

    fn update_mtime_exec(&self, nodeid: i64, mtime: i64) -> DbResult<()> {
        self.update_mtime_ns(nodeid, mtime)?;
        Ok(())
    }

    fn update_type_exec(&self, nodeid: i64, row_type: RowType) -> DbResult<()> {
        self.update_type(row_type as u8, nodeid)?;
        Ok(())
    }

    fn update_display_str_exec(&self, nodeid: i64, display_str: &str) -> DbResult<()> {
        self.update_node_display_str(display_str, nodeid)?;
        Ok(())
    }
    fn update_flags_exec(&self, nodeid: i64, flags: &str) -> DbResult<()> {
        self.update_node_flags(flags, nodeid)?;
        Ok(())
    }
    fn update_srcid_exec(&self, nodeid: i64, srcid: i64) -> DbResult<()> {
        self.update_node_srcid(nodeid, srcid)?;
        Ok(())
    }
    fn update_node_sha_exec(&self, nodeid: i64, sha: &str) -> DbResult<()> {
        self.insert_or_replace_node_sha(nodeid, sha)?;
        Ok(())
    }
    fn insert_link(&self, src: i64, dst: i64, is_sticky: u8, dst_type: RowType) -> DbResult<()> {
        self.insert_link_inner(src, dst, is_sticky, dst_type as u8)?;
        Ok(())
    }

    fn upsert_output_producer_link(
        &self,
        output_id: i64,
        producer_id: i64,
        _producer_type: RowType,
    ) -> DbResult<()> {
        // Enforce uniqueness: at most one producer per output.
        // Check for an existing producer edge to this output.
        let mut stmt = self.prepare(
            "SELECT from_id FROM NormalLink WHERE to_id = ?1 AND to_type IN (4, 7) LIMIT 1",
        )?;
        let mut rows = stmt.query([output_id])?;
        if let Some(row) = rows.next()? {
            let existing_from_id: i64 = row.get(0)?;
            if existing_from_id != producer_id {
                // Load names for a clearer error message where possible
                use crate::queries::LibSqlQueries as _;
                let existing_name = self
                    .fetch_node_name(existing_from_id)
                    .unwrap_or_else(|_| existing_from_id.to_string());
                let candidate_name = self
                    .fetch_node_name(producer_id)
                    .unwrap_or_else(|_| producer_id.to_string());
                let output_name = self
                    .fetch_node_name(output_id)
                    .unwrap_or_else(|_| output_id.to_string());
                return Err(AnyError::from(format!(
                    "Output {} already has producer {} (id={}), cannot also be produced by {} (id={})",
                    output_name, existing_name, existing_from_id, candidate_name, producer_id
                )));
            }
            // Idempotent: same producer already linked; ensure/update link properties (sticky)
            // to_type must be the output node's type (GenF/DirGen)
            use crate::queries::LibSqlQueries as _;
            let out_node = self
                .fetch_node_by_id(output_id)
                .map_err(|e| AnyError::from(e))?
                .ok_or_else(|| AnyError::from(format!("Output node id {} not found", output_id)))?;
            let out_ty = *out_node.get_type() as u8;
            self.insert_link_inner(producer_id, output_id, 1, out_ty)?;
            return Ok(());
        }
        // No existing producer; insert the new link (sticky)
        // to_type must be the output node's type (GenF/DirGen)
        use crate::queries::LibSqlQueries as _;
        let out_node = self
            .fetch_node_by_id(output_id)
            .map_err(|e| AnyError::from(e))?
            .ok_or_else(|| AnyError::from(format!("Output node id {} not found", output_id)))?;
        let out_ty = *out_node.get_type() as u8;
        self.insert_link_inner(producer_id, output_id, 1, out_ty)?;
        Ok(())
    }

    fn insert_trace(&self, path: &str, pid: i64, gen: i64, typ: u8, childcnt: i64) -> DbResult<()> {
        self.insert_trace_inner(path, pid, gen, typ, childcnt)?;
        Ok(())
    }
    fn mark_tupfile_entries(&self, tupfile_ids: &Vec<i64>) -> DbResult<()> {
        for tupfile_id in tupfile_ids {
            self.add_rules_and_outputs_of_tupfile_entities(*tupfile_id)?;
        }
        Ok(())
    }

    fn create_tupfile_entries_table(&self) -> DbResult<()> {
        self.create_tupfile_entities_table_inner()?;
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
    fn mark_rules_depending_on_modified_groups(&self) -> DbResult<()> {
        self.mark_rules_depending_on_modified_groups_inner()?;
        Ok(())
    }
    fn mark_dependent_tupfiles_of_tupfiles(&self) -> DbResult<()> {
        self.mark_dependent_tupfiles_of_tupfiles_inner()?;
        Ok(())
    }
    fn mark_dependent_tupfiles_of_glob(&self, glob_id: i64) -> DbResult<()> {
        self.mark_dependent_tupfiles_of_glob_inner(glob_id)?;
        Ok(())
    }

    fn replace_node_sha(&self, node_id: i64, mut sha: impl FnMut() -> String) -> DbResult<UpsertStatus>
    {
        let s =  self.fetch_saved_nodesha256(node_id);
        if s.is_err()
        {
            Ok(UpsertStatus::Unchanged(node_id))

        } else {
            let oldsha = sha();
            if oldsha.ne(&s.unwrap()) {
                self.update_node_sha_exec(node_id, oldsha.as_str())?;
                Ok(UpsertStatus::Updated(node_id))
            }else {
                Ok(UpsertStatus::Unchanged(node_id))
            }
        }
    }

    fn upsert_node_sha(&self, node_id: i64, sha: &str) -> DbResult<UpsertStatus> {
        enum Status { Present, Absent, PresentButDiff }
        let s =  self
            .fetch_saved_nodesha256(node_id)
            .map_or(Status::Absent,
                    |oldsha| if oldsha.ne(sha) { Status::PresentButDiff } else { Status::Present });
        match s {
            Status::Absent => {
                self.update_node_sha_exec(node_id, sha)?;
                Ok(UpsertStatus::Inserted(node_id))
            },
            Status::PresentButDiff => {
                self.update_node_sha_exec(node_id, sha)?;
                Ok(UpsertStatus::Updated(node_id))
            },
            Status::Present => Ok(UpsertStatus::Unchanged(node_id)),
        }
    }
    fn insert_monitored(&self, path: &str, gen_id: i64, event: i32) -> DbResult<()> {
        let _ = self.insert_monitored_inner(path, gen_id, event)?;
        Ok(())
    }

    fn insert_into_dirpathuf(&self, id: i64, dir: i64, name: &str) -> DbResult<()> {
        let _ = self.insert_into_dirpathbuf_inner(id, dir, name)?;
        Ok(())
    }
}

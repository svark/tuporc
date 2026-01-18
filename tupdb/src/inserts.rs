use crate::db::RowType::TupF;
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
<sql statements> each ending with ;
-- <eos>
...
! -> return type is () is used for insert/update/delete statements
? -> takes a closure as a parameter to process each row returned by the query
-> -> insert returns the id of the inserted row
*/

generate_prepared_statements!("sql/inserts.sql");
pub trait LibSqlInserts {
    fn insert_node(&self, n: &Node) -> DbResult<i64>;
    fn insert_node_compact(&self, n: &Node) -> DbResult<i64>;
    fn upsert_env_var(&self, name: &str, value: &str) -> DbResult<UpsertStatus>;
    fn update_columns(
        &self,
        node: &Node,
        existing_node: &Node,
        compute_sha256: impl FnMut() -> String,
    ) -> Result<u8, AnyError>;
    /// `upsert_node` is akin to the sqlite upsert operation
    /// for existing nodes it updates the node's columns, marking the node to the modify list/present list in this process.
    /// for new nodes it adds the node to Node table and marks the node in modify list/present list tables
    fn upsert_node_compact(
        &self,
        node: &Node,
        existing_node: &Node,
        compute_sha: impl FnMut() -> String,
        is_fresh_db: bool,
    ) -> Result<Node, AnyError>;
    fn upsert_node(
        &self,
        node: &Node,
        existing_node: (&Node,bool),
        f: impl FnMut() -> String,
    ) -> Result<Node, AnyError>;
    /// Add or update a node, returning the upserted node.
    /// If the node exists, its columns are updated as needed.
    /// If it is new, it is inserted.
    /// The compute_sha closure is called only if needed to compute the node's sha256.
    /// Returns the upserted Node.
    fn fetch_upsert_node_raw(
        &self,
        node: &Node,
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError>;
    fn mark_modified(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn mark_unborn(&self, id: i64) -> Result<()>;
    fn mark_rule_succeeded(&self, rule_id: i64) -> Result<()>;
    fn mark_present(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn mark_deleted(&self, id: i64, rtype: &RowType) -> Result<()>;
    fn update_mtime(&self, nodeid: i64, mtime: i64) -> DbResult<()>;
    fn update_node_type(&self, nodeid: i64, row_type: RowType) -> DbResult<()>;
    fn update_display_str(&self, nodeid: i64, display_str: &str) -> DbResult<()>;
    fn update_flags(&self, nodeid: i64, flags: &str) -> DbResult<()>;
    fn update_srcid(&self, nodeid: i64, srcid: i64) -> DbResult<()>;
    fn update_node_sha(&self, nodeid: i64, sha: &str) -> DbResult<()>;
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
    fn mark_tupfile_outputs(&self, tupfile_ids: impl Iterator<Item = i64>) -> DbResult<()>;
    fn mark_absent_nodes_to_delete(&self) -> DbResult<()>;
    fn mark_rules_with_changed_io(&self) -> DbResult<()>;
    fn mark_group_deps(&self) -> DbResult<()>;
    fn mark_tupfile_deps(&self) -> DbResult<()>;
    fn mark_glob_deps(&self, glob_id: i64) -> DbResult<()>;
    fn replace_node_sha(&self, node_id: i64, sha: impl FnMut() -> String)
        -> DbResult<UpsertStatus>;
    fn upsert_node_sha(&self, node_id: i64, sha: &str) -> DbResult<UpsertStatus>;
    fn insert_monitored(&self, path: &str, gen_id: i64, event: i32) -> DbResult<()>;
    fn insert_into_dirpathuf(&self, id: i64, dir: i64, name: &str) -> DbResult<()>;
    fn mark_missing_not_deleted(&self) -> DbResult<()>;
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
    fn insert_node_compact(&self, n: &Node) -> DbResult<i64> {
        let r = self.insert_node_compact_inner(
            n.get_dir(),
            n.get_name(),
            n.get_mtime(),
            *n.get_type() as u8,
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
        compute_sha256: impl FnMut() -> String,
    ) -> Result<u8, AnyError> {
        let mut modify = 0u8;
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
                if node.is_generated() && node.is_file() {
                    // a file is now recognized as generated for the first time
                    // update to unborn, a rule is expected to generate it
                    //self.mark_unborn(existing_node.get_id())?;
                    modify = 2;
                }
                //node_statements.update_node_type(existing_node.get_id(), *node.get_type())?;
                self.update_node_type(existing_node.get_id(), *node.get_type())?;
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
            if  modify == 0
            {
                modify = 1;
            }
            if self
                .replace_node_sha(existing_node.get_id(), compute_sha256)
                .as_ref()
                .is_ok_and(UpsertStatus::is_unchanged)
            {
                if  modify == 1  {
                    modify = 0u8;
                }
            }
        }
        if existing_node.get_display_str() != node.get_display_str() {
            log::debug!(
                "updating display_str for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_display_str(),
                node.get_display_str()
            );
            self.update_node_display_str(node.get_display_str(), existing_node.get_id())?;
            if  modify == 0  {
                modify = 1u8;
            }
        }
        if existing_node.get_flags() != node.get_flags() {
            log::debug!(
                "updating flags for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_flags(),
                node.get_flags()
            );
            self.update_node_flags(node.get_flags(), existing_node.get_id())?;
            if  modify == 0  {
                modify = 1u8;
            }
        }
        if existing_node.get_srcid() != node.get_srcid() {
            log::debug!(
                "updating srcid for:{}, {} -> {}",
                existing_node.get_name(),
                existing_node.get_srcid(),
                node.get_srcid()
            );
            self.update_node_srcid(node.get_srcid(), existing_node.get_id())?;
            if  modify == 0  {
                modify = 1u8;
            }
        }

        self.mark_present(existing_node.get_id(), existing_node.get_type())?;
        Ok(modify)
    }

    // same as upsert_node but uses compact insert (avoids display_str, flags, srcid)
    fn upsert_node_compact(
        &self,
        node: &Node,
        existing_node: &Node,
        compute_sha: impl FnMut() -> String,
        is_fresh_db: bool,
    ) -> Result<Node, AnyError> {
        let allow_modify = !is_fresh_db || node.get_type().eq(&TupF);
        if existing_node.is_valid() {
            let modify = self.update_columns(node, &existing_node, compute_sha)?;
            if modify == 1 && allow_modify {
                self.mark_modified(existing_node.get_id(), existing_node.get_type())?;
            }
            else if modify == 2 && allow_modify {
                self.mark_unborn(existing_node.get_id())?;
            }
            Ok(Node::copy_from(existing_node.get_id(), node))
        } else {
            let node = self
                .insert_node_compact(node)
                .map(|i| Node::copy_from(i, node))?;
            if allow_modify {
                self.mark_modified(node.get_id(), node.get_type())?;
            }
            self.mark_present(node.get_id(), node.get_type())?;
            // node sha is not computed unless needed for a rule
            Ok::<Node, AnyError>(node)
        }
    }

    fn upsert_node(
        &self,
        node: &Node,
        (existing_node, present): (&Node, bool),
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError> {
        if existing_node.is_valid() {
            let modify = self.update_columns(node, &existing_node, compute_sha)?;
            if modify == 1 {
                self.mark_modified(existing_node.get_id(), existing_node.get_type())?;
            }
            else if modify == 2 {
                self.mark_unborn(existing_node.get_id())?;
            }
            else if !present {
                self.mark_modified(existing_node.get_id(), existing_node.get_type())?;
            }
            Ok(Node::copy_from(existing_node.get_id(), node))
        } else {
            let node = self.insert_node(node).map(|i| Node::copy_from(i, node))?;
            if node.get_type() == &RowType::GenF {
                self.mark_unborn(node.get_id())?;
            }
            else {
                self.mark_modified(node.get_id(), node.get_type())?;
            }
            self.mark_present(node.get_id(), node.get_type())?;
            // node sha is not computed unless needed for a rule
            Ok::<Node, AnyError>(node)
        }
    }
    fn fetch_upsert_node_raw(
        &self,
        node: &Node,
        compute_sha: impl FnMut() -> String,
    ) -> Result<Node, AnyError> {
       let existing_node = self
            .fetch_node_by_dir_and_name_raw(node.get_dir(), node.get_name())
            .unwrap_or(Node::unknown());
       let alive_node = existing_node.is_valid() && self.is_node_present(existing_node.get_id());
       if existing_node.get_srcid() != -1 && node.get_srcid() == -1 && alive_node {
            if existing_node.get_srcid() != node.get_srcid() {
                // fetch previously stored rule name for better error message
                let name = self
                    .fetch_node_name(existing_node.get_srcid())
                    .unwrap_or("unknown".to_string());
                let cur_name = self
                    .fetch_node_name(node.get_srcid())
                    .unwrap_or("unknown".to_string());
                return Err(AnyError::ConflictingParents(
                    existing_node.get_srcid(),
                    existing_node.get_name().to_string()
                        + " (previously created by rule: '"
                        + name.as_str()
                        + "' now by rule: '"
                        + cur_name.as_str()
                        + "')",
                ));
            }
        }
        self.upsert_node(node, (&existing_node, alive_node), compute_sha)
    }
    fn mark_modified(&self, id: i64, rtype: &RowType) -> Result<()> {
        self.add_to_modify_list(id, *rtype as u8)?;
        Ok(())
    }
    fn mark_unborn(&self, id: i64) -> Result<()> {
        self.add_to_unborn_list(id)?;
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

    fn update_mtime(&self, nodeid: i64, mtime: i64) -> DbResult<()> {
        self.update_mtime_ns(nodeid, mtime)?;
        Ok(())
    }

    fn update_node_type(&self, nodeid: i64, row_type: RowType) -> DbResult<()> {
        self.update_node_type_raw(row_type as u8, nodeid)?;
        Ok(())
    }

    fn update_display_str(&self, nodeid: i64, display_str: &str) -> DbResult<()> {
        self.update_node_display_str(display_str, nodeid)?;
        Ok(())
    }
    fn update_flags(&self, nodeid: i64, flags: &str) -> DbResult<()> {
        self.update_node_flags(flags, nodeid)?;
        Ok(())
    }
    fn update_srcid(&self, nodeid: i64, srcid: i64) -> DbResult<()> {
        self.update_node_srcid(nodeid, srcid)?;
        Ok(())
    }
    fn update_node_sha(&self, nodeid: i64, sha: &str) -> DbResult<()> {
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
            "SELECT from_id FROM NormalLink WHERE to_id =?1 AND to_type IN(4, 7) LIMIT 1",
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
                    "Output {} already has producer {}(id = {}), cannot also be produced by {}(id = {})",
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
    fn mark_tupfile_outputs(&self, mut tupfile_ids: impl Iterator<Item = i64>) -> DbResult<()> {
        tupfile_ids.try_for_each(|tupfile_id| -> DbResult<()> {
            self.add_rules_to_tupfile_entities(tupfile_id)?;
            Ok(())
        })?;

        self.add_outputs_to_tupfile_entities_inner()?;
        Ok(())
    }

    fn mark_absent_nodes_to_delete(&self) -> DbResult<()> {
        // If PresentList is empty (e.g., skip-scan), skip to avoid mass-deleting everything.
        let present_count: i64 = self
            .query_row("SELECT COUNT(1) FROM PresentList", [], |row| row.get(0))
            .unwrap_or(0);
        if present_count == 0 {
            log::warn!(
                "PresentList is empty; skipping mark_absent_nodes_to_delete to avoid mass deletes"
            );
            return Ok(());
        }
        self.add_not_present_to_delete_list_inner()?;
        Ok(())
    }
    fn mark_rules_with_changed_io(&self) -> DbResult<()> {
        self.add_rules_with_changed_io_to_modify_list_inner()?;
        Ok(())
    }
    fn mark_group_deps(&self) -> DbResult<()> {
        self.mark_rules_depending_on_modified_groups_inner()?;
        Ok(())
    }
    fn mark_tupfile_deps(&self) -> DbResult<()> {
        self.mark_dependent_tupfiles_of_tupfiles_inner()?;
        Ok(())
    }
    fn mark_glob_deps(&self, glob_id: i64) -> DbResult<()> {
        self.mark_dependent_tupfiles_of_glob_inner(glob_id)?;
        Ok(())
    }

    fn replace_node_sha(
        &self,
        node_id: i64,
        mut sha: impl FnMut() -> String,
    ) -> DbResult<UpsertStatus> {
        let oldsha = self.fetch_saved_nodesha256(node_id)?;
        let newsha = sha();
        if newsha.is_empty() {
            return Err(AnyError::ShaError(format!("Empty sha for {}", node_id)));
        }
        if newsha.ne(&oldsha) {
            self.update_node_sha(node_id, newsha.as_str())?;
            Ok(UpsertStatus::Updated(node_id))
        } else {
            Ok(UpsertStatus::Unchanged(node_id))
        }
    }

    fn upsert_node_sha(&self, node_id: i64, sha: &str) -> DbResult<UpsertStatus> {
        enum Status {
            Present,
            Absent,
            PresentButDiff,
        }
        let s = self
            .fetch_saved_nodesha256(node_id)
            .map_or(Status::Absent, |oldsha| {
                if oldsha.ne(sha) {
                    Status::PresentButDiff
                } else {
                    Status::Present
                }
            });
        match s {
            Status::Absent => {
                self.update_node_sha(node_id, sha)?;
                Ok(UpsertStatus::Inserted(node_id))
            }
            Status::PresentButDiff => {
                self.update_node_sha(node_id, sha)?;
                Ok(UpsertStatus::Updated(node_id))
            }
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
     fn mark_missing_not_deleted(&self) -> DbResult<()> {
        self.mark_missing_not_deleted_inner()?;
         Ok(())
    }
}

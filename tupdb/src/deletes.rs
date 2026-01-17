use rusqlite::{Connection, Result};
use tupdb_sql_macro::generate_prepared_statements;

generate_prepared_statements!("sql/deletes.sql");

pub trait LibSqlDeletes {
    fn delete_link(&self, id: i64) -> Result<usize>;
    fn delete_node_sha(&self, id: i64) -> Result<usize>;
    fn delete_messages(&self) -> Result<usize>;
    fn delete_monitored_by_generation_id(&self, id: i64) -> Result<usize>;
    fn unmark_modified(&self, id: i64) -> Result<usize>;
    fn delete_nodes(&self) -> Result<usize>;
    fn mark_orphan_dirgen_nodes(&self) -> Result<usize>;

    fn mark_absent_tupfile_entries_to_delete(&self) -> Result<()>;

    fn mark_orphans_to_delete(&self) -> Result<()>;

    fn prune_delete_of_present_list(&self) -> Result<usize>;
    fn prune_delete_list(&self) -> Result<usize>;
    fn prune_modify_list(&self) -> Result<usize>;

    /// Enrich DeleteList by adding nodes whose parent directory is already in DeleteList
    fn mark_dir_dependents_to_delete(&self) -> Result<usize>;

    /// Enrich DeleteList by adding nodes whose parent directories are missing from Node (orphaned dirid)
    fn mark_missing_dirs_to_delete(&self) -> Result<usize>;

    fn drop_tupfile_entries_table(&self) -> Result<()>;
}

impl LibSqlDeletes for Connection {
    fn delete_link(&self, id: i64) -> Result<usize> {
        let sz = self.delete_link_inner(id, id)?;
        log::debug!("Deleted {} row(s) from link", sz);
        Ok(sz)
    }

    fn delete_node_sha(&self, id: i64) -> Result<usize> {
        let sz = self.delete_node_sha_inner(id)?;
        log::debug!("Deleted {} row(s) from node_sha", sz);
        Ok(sz)
    }

    fn delete_messages(&self) -> Result<usize> {
        let sz = self.delete_messages_inner()?;
        log::debug!("Deleted {} rows from messages", sz);
        Ok(sz)
    }

    fn delete_monitored_by_generation_id(&self, id: i64) -> Result<usize> {
        let sz = self.delete_monitored_files_by_generation_id_inner(id)?;
        log::debug!("Deleted {} rows from monitored_files", sz);
        Ok(sz)
    }

    fn unmark_modified(&self, id: i64) -> Result<usize> {
        let rows_deleted = self.unmark_modified_inner(id)?;
        log::debug!("Deleted {} rows from modified", rows_deleted);
        Ok(rows_deleted)
    }

    fn delete_nodes(&self) -> Result<usize> {
        let sz = self.delete_nodes_inner()?;
        log::debug!("Deleted {} rows from nodes", sz);
        Ok(sz)
    }

    fn mark_orphan_dirgen_nodes(&self) -> Result<usize> {
        let sz = self.mark_orphan_dirgen_nodes_inner()?;
        log::debug!("Deleted {} orphan DirGen nodes", sz);
        Ok(sz)
    }

    fn mark_absent_tupfile_entries_to_delete(&self) -> Result<()> {
        self.delete_tupfile_entries_not_in_present_list_inner()?;
        log::debug!("Deleted tupfile_entries not in PresentList table");
        Ok(())
    }
    fn mark_orphans_to_delete(&self) -> Result<()> {
        self.delete_orphaned_tupentries_inner()?;
        log::debug!("Deleted orphaned tupfile_entries");
        Ok(())
    }

    fn prune_delete_of_present_list(&self) -> Result<usize> {
        let sz = self.prune_delete_list_of_present_inner()?;
        log::debug!("Deleted {} rows from delete_list_of_present", sz);
        Ok(sz)
    }
    fn prune_delete_list(&self) -> Result<usize> {
        let sz = self.prune_delete_list_inner()?;
        log::debug!("Deleted {} rows from delete_list", sz);
        Ok(sz)
    }

    fn prune_modify_list(&self) -> Result<usize> {
        let sz = self.prune_modify_list_of_inputs_and_outputs_inner()?;
        log::debug!("Deleted {} rows from modify_list_of_inputs_and_outputs", sz);
        Ok(sz)
    }
    fn mark_dir_dependents_to_delete(&self) -> Result<usize> {
        let sz = self.enrich_delete_list_with_dir_dependents_inner()?;
        log::debug!("Enriched DeleteList with {} children of deleted dirs", sz);
        Ok(sz)
    }
    fn mark_missing_dirs_to_delete(&self) -> Result<usize> {
        let sz = self.enrich_delete_list_for_missing_dirs_inner()?;
        log::debug!(
            "Enriched DeleteList with {} nodes having missing parent dirs",
            sz
        );
        Ok(sz)
    }
    fn drop_tupfile_entries_table(&self) -> Result<()> {
        let _ = self.drop_tupfile_entities_table_inner()?;
        log::debug!("Dropped tupfile_entries table");
        Ok(())
    }

}

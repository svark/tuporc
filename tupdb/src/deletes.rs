use rusqlite::{Connection, Result};
use tupdb_sql_macro::generate_prepared_statements;

generate_prepared_statements!("sql/deletes.sql");

pub trait LibSqlDeletes {
    fn delete_node(&self, id: i64) -> Result<usize>;
    fn delete_link(&self, id: i64) -> Result<usize>;
    fn delete_node_sha(&self, id: i64) -> Result<usize>;
    fn delete_messages(&self) -> Result<usize>;
    fn delete_monitored_by_generation_id(&self, id: i64) -> Result<usize>;
    fn delete_modified(&self, id: i64) -> Result<usize>;
    fn delete_nodes(&self) -> Result<usize>;
    fn prune_delete_list_of_present(&self) -> Result<usize>;
    fn prune_modify_list_of_inputs_and_outputs(&self) -> Result<usize>;

    fn drop_tupfile_entries_table(&self) -> Result<()>;
}

impl LibSqlDeletes for Connection {
    fn delete_node(&self, id: i64) -> Result<usize> {
        let sz = self.delete_node_inner(id)?;
        log::debug!("Deleted {} row(s) from node", sz);
        Ok(sz)
    }

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

    fn delete_modified(&self, id: i64) -> Result<usize> {
        let rows_deleted = self.delete_modified_inner(id)?;
        log::debug!("Deleted {} rows from modified", rows_deleted);
        Ok(rows_deleted)
    }

    fn delete_nodes(&self) -> Result<usize> {
        let sz = self.delete_nodes_inner()?;
        log::debug!("Deleted {} rows from nodes", sz);
        Ok(sz)
    }

    fn prune_delete_list_of_present(&self) -> Result<usize> {
        let sz = self.prune_delete_list_of_present_inner()?;
        log::debug!("Deleted {} rows from delete_list_of_present", sz);
        Ok(sz)
    }

    fn prune_modify_list_of_inputs_and_outputs(&self) -> Result<usize> {
        let sz = self.prune_modify_list_of_inputs_and_outputs_inner()?;
        log::debug!("Deleted {} rows from modify_list_of_inputs_and_outputs", sz);
        Ok(sz)
    }
    fn drop_tupfile_entries_table(&self) -> Result<()> {
        let _ = self.drop_tupfile_entities_table_inner()?;
        log::debug!("Dropped tupfile_entries table");
        Ok(())
    }
}

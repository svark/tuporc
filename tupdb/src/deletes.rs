use include_sqlite_sql::{impl_sql, include_sql};
use rusqlite::{Connection, Result};

include_sql!("src/sql/deletes.sql");

pub trait LibSqlDeletes {
    fn delete_node(&self, id: i64) -> Result<()>;
    fn delete_link(&self, id: i64) -> Result<()>;
    fn delete_node_sha(&self, id: i64) -> Result<()>;
    fn delete_messages(&self) -> Result<()>;
    fn delete_monitored_by_generation_id(&self, id: i64) -> Result<()>;
    fn delete_modified(&self, id: i64) -> Result<()>;
    fn delete_nodes(&self) -> Result<()>;
    fn prune_delete_list_of_present(&self) -> Result<()>;
    fn prune_modify_list_of_inputs_and_outputs(&self) -> Result<()>;
}

impl LibSqlDeletes for Connection {
    fn delete_node(&self, id: i64) -> Result<()> {
        self.delete_node_inner(id)?;
        Ok(())
    }

    fn delete_link(&self, id: i64) -> Result<()> {
        self.delete_link_inner(id)?;
        Ok(())
    }

    fn delete_node_sha(&self, id: i64) -> Result<()> {
        self.delete_node_sha_inner(id)?;
        Ok(())
    }

    fn delete_messages(&self) -> Result<()> {
        self.delete_messages_inner()?;
        Ok(())
    }

    fn delete_monitored_by_generation_id(&self, id: i64) -> Result<()> {
        self.delete_monitored_files_by_gen_id_inner(id)?;
        Ok(())
    }

    fn delete_modified(&self, id: i64) -> Result<()> {
        self.delete_from_modifylist_inner(id)?;
        Ok(())
    }

    fn delete_nodes(&self) -> Result<()> {
        self.delete_nodes_inner()?;
        Ok(())
    }

    fn prune_delete_list_of_present(&self) -> Result<()> {
        self.prune_delete_list_of_present_inner()?;
        Ok(())
    }
    fn prune_modify_list_of_inputs_and_outputs(&self) -> Result<()> {
        self.prune_modify_list_of_inputs_and_outputs_inner()?;
        Ok(())
    }
    

}

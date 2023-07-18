use std::cmp::Ordering;
use std::fs::File;
use std::path::{MAIN_SEPARATOR, Path, PathBuf};

use eyre::Result;
use log::debug;
use pathdiff::diff_paths;
use rusqlite::{Connection, Params, Statement};

use tupparser::decode::MatchingPath;

use crate::{make_node, make_tup_node};
use crate::db::StatementType::*;

#[repr(u8)]
#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub enum RowType {
    /// File
    File = 0,
    /// Rule
    Rule = 1,
    /// Directory
    Dir = 2,
    /// Env var
    Env = 3,
    /// Generated file
    GenF = 4,
    /// Tupfile or Tupfile.lua
    TupF = 5,
    /// Group
    Grp = 6,
    /// Generated Directory
    DirGen = 7,
    /// Excluded file pattern
    Excluded = 8,
}

impl PartialOrd<Self> for RowType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        (*self as u8).partial_cmp(&(*other as u8))
    }
}

impl Ord for RowType {
    fn cmp(&self, other: &Self) -> Ordering {
        (*self as u8).cmp(&(*other as u8))
    }
}

const ENVDIR: i8 = -2;

impl ToString for RowType {
    fn to_string(&self) -> String {
        (*self as u8).to_string()
    }
}

impl TryFrom<u8> for RowType {
    type Error = u8;
    fn try_from(value: u8) -> std::result::Result<Self, u8> {
        if value == 0 {
            Ok(Self::File)
        } else if value == 1 {
            Ok(Self::Rule)
        } else if value == 2 {
            Ok(Self::Dir)
        } else if value == 3 {
            Ok(Self::Env)
        } else if value == 4 {
            Ok(Self::GenF)
        } else if value == 5 {
            Ok(Self::TupF)
        } else if value == 6 {
            Ok(Self::Grp)
        } else if value == 7 {
            Ok(Self::DirGen)
        } else if value == 8 {
            Ok(Self::Excluded)
        } else {
            Err(value)
        }
    }
}
/// Fields in the Node table in the database
#[derive(Clone, Debug)]
pub struct Node {
    id: i64,
    pid: i64,
    //< Parent folder id
    mtime: i64,
    // time in nano s
    name: String,
    // name of file or dir or rule or group
    rtype: RowType,
    // type of data in this row
    display_str: String,
    // rule display
    flags: String,
    // flags for rule that appear in description
    srcid: i64, //< wherefrom this node came about
}

impl PartialEq<Self> for Node {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Node {}

impl PartialOrd<Self> for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.get_id())
    }
}
impl Node {
    pub fn new(id: i64, pid: i64, mtime: i64, name: String, rtype: RowType) -> Node {
        Node {
            id,
            pid,
            mtime,
            name,
            rtype,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid: -1,
        }
    }
    pub fn copy_from(id: i64, n: &Node) -> Node {
        Node {
            id,
            pid: n.pid,
            mtime: n.mtime,
            name: n.name.clone(),
            rtype: n.rtype,
            display_str: n.display_str.clone(),
            flags: n.flags.clone(),
            srcid: n.srcid,
        }
    }
    pub fn new_file_or_genf(
        id: i64,
        pid: i64,
        mtime: i64,
        name: String,
        rtype: RowType,
        srcid: i64,
    ) -> Node {
        Node {
            id,
            pid,
            mtime,
            name,
            rtype,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid,
        }
    }
    pub fn new_rule(id: i64, pid: i64, name: String, display_str: String, flags: String) -> Node {
        Node {
            id,
            pid,
            mtime: 0,
            name,
            rtype: RowType::Rule,
            display_str,
            flags,
            srcid: -1,
        }
    }
    pub fn new_grp(id: i64, pid: i64, name: String) -> Node {
        Node {
            id,
            pid,
            mtime: 0,
            name,
            rtype: RowType::Grp,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid: -1,
        }
    }
    pub fn get_id(&self) -> i64 {
        self.id
    }
    pub fn get_pid(&self) -> i64 {
        self.pid
    }
    pub fn get_mtime(&self) -> i64 {
        self.mtime
    }
    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }
    pub fn get_type(&self) -> &RowType {
        &self.rtype
    }
    pub fn get_display_str(&self) -> &str {
        self.display_str.as_str()
    }
    pub fn get_flags(&self) -> &str {
        self.flags.as_str()
    }
    pub fn get_srcid(&self) -> i64 {
        self.srcid
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StatementType {
    AddToMod,
    AddToDel,
    AddToPres,
    AddToTempIds,
    InsertDir,
    InsertDirAux,
    InsertFile,
    InsertEnv,
    InsertLink,
    //InsertFnTrace,
    FindDirId,
    FindGroupId,
    FindNode,
    FindNodeId,
    FindNodeById,
    FindEnvId,
    FindNodes,
    FindGlobNodes,
    FindNodePath,
    FindNodeByPath,
    FindParentRule,
    //FindIo,
    UpdMTime,
    UpdDirId,
    UpdEnv,
    DeleteId,
    DeleteNormalRuleLinks,
    ImmediateDeps,
    RuleDepRules,
    RestoreDeleted,
    FetchIO,
    FetchEnvsForRule,
    RemovePresents,
    FetchOutputsForRule,
    FetchInputsForRule,
}

pub struct SqlStatement<'conn> {
    stmt: Statement<'conn>,
    tok: StatementType,
}

pub(crate) trait LibSqlPrepare {
    fn add_to_modify_prepare(&self) -> Result<SqlStatement>;
    fn add_to_delete_prepare(&self) -> Result<SqlStatement>;
    fn add_to_present_prepare(&self) -> Result<SqlStatement>;
    fn remove_presents_prepare(&self) -> Result<SqlStatement>;
    fn insert_dir_prepare(&self) -> Result<SqlStatement>;
    fn insert_dir_aux_prepare(&self) -> Result<SqlStatement>;
    fn insert_link_prepare(&self) -> Result<SqlStatement>;
    fn insert_node_prepare(&self) -> Result<SqlStatement>;
    fn insert_env_prepare(&self) -> Result<SqlStatement>;
    fn fetch_dirid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_groupid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_prepare(&self) -> Result<SqlStatement>;
    fn fetch_nodeid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_by_id_prepare(&self) -> Result<SqlStatement>;
    fn fetch_env_id_prepare(&self) -> Result<SqlStatement>;
    fn fetch_rules_nodes_prepare_by_dirid(&self) -> Result<SqlStatement>;
    fn fetch_glob_nodes_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_path_prepare(&self) -> Result<SqlStatement>;
    fn fetch_parent_rule_prepare(&self) -> Result<SqlStatement>;
    fn find_node_by_path_prepare(&self) -> Result<SqlStatement>;
    fn update_mtime_prepare(&self) -> Result<SqlStatement>;
    fn update_dirid_prepare(&self) -> Result<SqlStatement>;
    fn update_env_prepare(&self) -> Result<SqlStatement>;
    fn delete_prepare(&self) -> Result<SqlStatement>;
    fn delete_tup_rule_links_prepare(&self) -> Result<SqlStatement>;
    fn mark_outputs_deleted_prepare(&self) -> Result<SqlStatement>;
    fn get_rule_deps_tupfiles_prepare(&self) -> Result<SqlStatement>;
    fn restore_deleted_prepare(&self) -> Result<SqlStatement>;
    fn mark_deleted_prepare(&self) -> Result<SqlStatement>;
    fn fetch_io_prepare(&self) -> Result<SqlStatement>;
    fn fetch_envs_for_rule_prepare(&self) -> Result<SqlStatement>;
    fn fetch_outputs_for_rule_prepare(&self) -> Result<SqlStatement>;
    fn fetch_inputs_for_rule_prepare(&self) -> Result<SqlStatement>;
}

pub(crate) trait LibSqlExec {
    /// Add a node to the modify list. These are nodes(files rules, etc) that have been modified since the last run
    fn add_to_modify_exec(&mut self, node_id: i64, rtyp: RowType) -> Result<()>;
    /// Add a node to the delete list. These are nodes(files rules, etc) that have been deleted since the last run
    fn add_to_delete_exec(&mut self, node_id: i64, rtype: RowType) -> Result<()>;
    /// Add a node to the present list. These are nodes(files rules, etc) that still exist after the last run
    fn add_to_present_exec(&mut self, node_id: i64, rtype: RowType) -> Result<()>;
    fn add_to_temp_ids_table_exec(&mut self, id: i64, rtype: RowType) -> Result<()>;
    fn remove_presents_exec(&mut self) -> Result<()>;
    /// Insert a directory node into the database
    fn insert_dir_exec(&mut self, path_str: &str, dir: i64) -> Result<i64>;
    /// Insert a directory node into the database. This version is used to keep track of full paths of the dirs in an auxillary table
    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()>;
    /// Insert a link between two nodes into the database. This is used to keep track of the dependencies between nodes and build a graph
    fn insert_link(&mut self, from_id: i64, to_id: i64, is_sticky: bool, to_type: RowType) -> Result<()>;
    /// Insert a sticky link between two nodes into the database. This is used to keep track of the dependencies between nodes and build a graph
    //fn insert_sticky_link(&mut self, from_id: i64, to_id: i64) -> Result<()>;
    /// insert a node into the database
    fn insert_node_exec(&mut self, n: &Node) -> Result<i64>;
    /// Insert an env key value pair into the database
    fn insert_env_exec(&mut self, env: &str, val: &str) -> Result<i64>;
    /// fetch directory node id from its path.
    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64>;
    /// fetch group id from its name and directory
    fn fetch_group_id(&mut self, node_name: &str, dir: i64) -> Result<i64>;
    /// fetch a node from its name and directory
    fn fetch_node(&mut self, node_name: &str, dir: i64) -> Result<Node>;
    /// fetch a node by its id
    fn fetch_node_by_id(&mut self, i: i64) -> Result<Node>;
    /// fetch id of a node from its name and directory
    fn fetch_node_id(&mut self, node_name: &str, dir: i64) -> Result<i64>;
    /// fetch id of an env key from its name
    fn fetch_env_id(&mut self, env_name_val: &str) -> Result<(i64, String)>;
    /// fetch all the nodes that are rules and are in the given directory
    fn fetch_rule_nodes_by_dirid(&mut self, dir: i64) -> Result<Vec<Node>>;
    /// fetch all the nodes that match the given glob pattern and are in the given directory
    fn fetch_glob_nodes<F, P>(&mut self, base_path: P, gname: P, f: F) -> Result<Vec<MatchingPath>>
        where
            F: FnMut(&String) -> Option<MatchingPath>,
            P: AsRef<Path>;
    /// fetch rule that produces the given node
    fn fetch_parent_rule(&mut self, id: i64) -> Result<Vec<i64>>;
    /// fetch node path from its name and directory id
    fn fetch_node_path(&mut self, name: &str, dirid: i64) -> Result<PathBuf>;
    /// update modified time for a node with the given id
    fn update_mtime_exec(&mut self, id: i64, mtime_ns: i64) -> Result<()>;
    /// update directory id for a node with the given id
    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()>;
    /// update env value  for a env node with the given id
    fn update_env_exec(&mut self, id: i64, val: String) -> Result<()>;
    /// delete a node with the given id
    fn delete_exec(&mut self, id: i64) -> Result<()>;
    /// deleted nodes that are outputs of a rule with the given rule id
    fn mark_rule_outputs_deleted(&mut self, rule_id: i64) -> Result<()>;
    /// fetch all tupfiles that depend on the given rule (usually via a group output)
    fn fetch_dependant_tupfiles(&mut self, rule_id: i64) -> Result<Vec<Node>>;
    /// delete normal links between nodes that are inputs and outputs of a rule with the given rule id
    fn delete_normal_rule_links(&mut self, rule_id: i64) -> Result<()>;
    /// find node by path
    fn find_node_by_path<P: AsRef<Path>>(&mut self, dir_path: P, name: &str) -> Result<(i64, i64)>;
    /// fetch all read write operations performed by a process with given pid
    fn fetch_io(&mut self, proc_id: i32) -> Result<Vec<(String, u8)>>;
    /// fetch all envs necessary for the given rule
    fn fetch_envs(&mut self, rule_id: i32) -> Result<Vec<String>>;
    /// fetch all inputs of the given rule
    fn fetch_inputs(&mut self, rule_id: i32) -> Result<Vec<Node>>;
    /// fetch all outputs of the given rule
    fn fetch_outputs(&mut self, rule_id: i32) -> Result<Vec<Node>>;
}
pub(crate) trait MiscStatements {
    fn populate_delete_list(&self) -> Result<()>;
    fn enrich_modified_list(&self) -> Result<()>;
    fn remove_id_from_delete_list(&self, id: i64) -> Result<()>;
    fn prune_present_list(&self) -> Result<()>;
    fn prune_modified_list(&self) -> Result<()>;
    fn insert_trace<P: AsRef<Path>>(&mut self, filepath: P, pid: u32, gen: u32, typ: i8, childcnt: i32) -> Result<i64>;
    fn remove_modified_list(&self) -> Result<()>;
}
pub(crate) trait ForEachClauses {
    fn for_each_file_node_id<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(i64) -> Result<()>;
    fn for_each_node_id<P, F>(&self, p: P, f: F) -> Result<()>
    where
        P: Params,
        F: FnMut(i64) -> Result<()>;
    fn for_changed_or_created_tup_node_with_path<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>;

    fn for_each_grp_nodeid_provider<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(i64) -> Result<()>;
    fn for_each_grp_node_provider<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>;
    fn for_each_grp_node_requirer<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>;
    fn fetch_db_sticky_inputs(&self, rule_id: i64) -> Result<Vec<i64>>;
    fn fetch_db_inputs(&self, rule_id: i64) -> Result<Vec<i64>>;
    fn fetch_db_outputs(&self, rule_id: i64) -> Result<Vec<i64>>;

    fn for_tupid_and_path<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
        where
            P: Params,
            F: FnMut(Node) -> Result<()>;
    fn for_id<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
        where
            P: Params,
            F: FnMut(i64) -> Result<()>;
    fn for_node<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
        where
            P: Params,
            F: FnMut(Node) -> Result<()>;
    fn rule_link(rule_id: i64, stmt: &mut Statement) -> Result<Vec<i64>>;
    fn for_each_link<F>(&mut self, f: F) -> Result<()>
        where F: FnMut(i32, i32) -> Result<()>;
    //fn populate_delete_list(&self) -> Result<()>;
    fn rules_to_run_no_target(&self) -> Result<Vec<Node>>;
    fn get_target_ids(&self, root: &Path, target_ids: &Vec<String>) -> Result<Vec<i64>>;
}

// Check if the node table exists in .tup/db
pub fn is_initialized(conn: &Connection, table_name: &str) -> bool {
    if let Ok(mut stmt) =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?; ")
    {
        stmt.query_row([table_name], |_x| Ok(true)).is_ok()
    } else {
        false
    }
}

// Handle the tup init subcommand. This creates the file .tup\db and adds the tables
pub fn init_db() {
    println!("Creating a new db.");
    //use std::fs;
    std::fs::create_dir_all(".tup").expect("Unable to access .tup dir");
    let conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
    conn.execute_batch(
        include_str!("sql/node_table.sql")
    )
        .expect("failed to create tables in tup database.");

    let _ = File::create("Tupfile.ini").expect("could not open Tupfile.ini for write");
    println!("Finished creating tables");
}

// create a temp table from directories paths to their node ids
pub fn create_path_buf_temptable(conn: &Connection) -> Result<()> {
    // https://gist.github.com/jbrown123/b65004fd4e8327748b650c77383bf553
    //let dir : u8 = RowType::Dir as u8;
    let s = include_str!("sql/dirpathbuf_temptable.sql");
    conn.execute_batch(s)?;
    Ok(())
}


pub fn create_dyn_io_temp_tables(conn: &Connection) -> Result<()> {
    conn.execute_batch(include_str!("sql/dynio.sql"))?;
    Ok(())
}

//creates a temp table
pub fn create_temptables(conn: &Connection) -> Result<()> {
    let stmt = include_str!("sql/temptables.sql");
    conn.execute_batch(stmt)?;
    Ok(())
}

impl LibSqlPrepare for Connection {
    fn add_to_modify_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT or IGNORE into ModifyList(id, type) Values (?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: AddToMod,
        })
    }
    fn add_to_delete_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT or IGNORE into DeleteList(id, type) Values (?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: AddToDel,
        })
    }
    fn add_to_present_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT or IGNORE into PresentList(id, type) Values(?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: AddToPres,
        })
    }

    fn remove_presents_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(
            "DELETE from DeleteList where id in (SELECT id from PresentList)"
        )?;
        Ok(SqlStatement {
            stmt,
            tok: RemovePresents,
        })
    }

    fn insert_dir_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into Node (name, dir, type) Values (?,?,?);")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertDir,
        })
    }
    fn insert_dir_aux_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into DirPathBuf (id, name) Values (?,?);")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertDirAux,
        })
    }


    fn insert_link_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT OR IGNORE into NormalLink (from_id, to_id, issticky, to_type) Values (?,?,?, ?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertLink,
        })
    }

    fn insert_node_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into Node (dir, name, mtime_ns, type, display_str, flags, srcid) Values (?,?,?,?,?,?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertFile,
        })
    }

    fn insert_env_prepare(&self) -> Result<SqlStatement> {
        let rtype = RowType::Env as u8;
        let e = ENVDIR;
        let stmt = self.prepare(&*format!("INSERT into Node (dir, name, type, display_str) Values ({e}, ?, {rtype}, ?)"))?;
        Ok(SqlStatement {
            stmt,
            tok: InsertEnv,
        })
    }

    fn fetch_dirid_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT id FROM DirPathBuf where name=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindDirId,
        })
    }
    fn fetch_groupid_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT id from Node where  dir=? and name='?'")?;
        Ok(SqlStatement {
            stmt,
            tok: FindGroupId,
        })
    }

    fn fetch_node_prepare(&self) -> Result<SqlStatement> {
        let stmt =
            self.prepare("SELECT id, dir, type, name, mtime_ns FROM Node where dir=? and name=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNode,
        })
    }
    fn fetch_nodeid_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT id FROM Node where dir=? and name=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodeId,
        })
    }

    fn fetch_node_by_id_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT id,dir, mtime_ns, name, type FROM Node where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodeById,
        })
    }

    fn fetch_env_id_prepare(&self) -> Result<SqlStatement> {
        let e = ENVDIR;
        let stmt = self.prepare(&*format!("Select id, display_str from node where name=? and dir={e}"))?;
        Ok(SqlStatement {
            stmt,
            tok: FindEnvId,
        })
    }

    fn fetch_rules_nodes_prepare_by_dirid(&self) -> Result<SqlStatement> {
        let rtype = RowType::Rule as u8;
        let stmt = self.prepare(&*format!(
            "SELECT id, dir, type, name, mtime_ns FROM Node where dir=? and type={rtype}"
        ))?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodes,
        })
    }

    fn fetch_glob_nodes_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT name from Node where name like ? and dir in (SELECT id FROM DirPathBuf where name = ?)")?;
        Ok(SqlStatement {
            stmt,
            tok: FindGlobNodes,
        })
    }

    fn fetch_node_path_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT name from DirPathBuf where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodePath,
        })
    }

    fn fetch_parent_rule_prepare(&self) -> Result<SqlStatement> {
        let rty = RowType::Rule as u8;
        let stmtstr = format!(
            "SELECT id FROM Node where type={} and id in \
        (SELECT from_id from NormalLink where to_id = ?)",
            rty
        );
        let stmt = self.prepare(stmtstr.as_str())?;
        Ok(SqlStatement {
            stmt,
            tok: FindParentRule,
        })
    }
    fn find_node_by_path_prepare(&self) -> Result<SqlStatement> {
        let stmtstr = format!("SELECT Node.id, Node.dir from Node where Node.name = ? and dir in \
        (SELECT id from DIRPATHBUF where DIRPATHBUF.name=?)");
        let stmt = self.prepare(stmtstr.as_str())?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodeByPath,
        })
    }

    fn update_mtime_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set mtime_ns = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdMTime,
        })
    }

    fn update_dirid_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set dir = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdDirId,
        })
    }
    fn update_env_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("Update Node Set display_str = ? where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdEnv,
        })
    }

    fn delete_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE FROM Node WHERE id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: DeleteId,
        })
    }

    fn delete_tup_rule_links_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE from NormalLink where from_id = ? or to_id  = ?")?;
        Ok(
            SqlStatement {
                stmt,
                tok: DeleteNormalRuleLinks,
            },
        )
    }

    fn mark_outputs_deleted_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(
            " INSERT into DeleteList (id) \
             SELECT to_id from NormalLink where from_id=?",
        )?;
        Ok(SqlStatement {
            stmt,
            tok: ImmediateDeps,
        })
    }


    fn get_rule_deps_tupfiles_prepare(&self) -> Result<SqlStatement> {
        let rtype = RowType::Rule as u8;
        let stmt = self.prepare(&*format!(
            "SELECT id, dir, name from TUPPATHBUF where dir in
             (SELECT dir from Node where type = {rtype} and id in
            (WITH RECURSIVE dependants(x) AS (
   SELECT ?
   UNION
  SELECT to_id FROM NormalLink JOIN dependants ON NormalLink.from_id=x
)
SELECT DISTINCT x FROM dependants));",
        ))?;

        // placeholder ? here is the rule_id which initiates the search (base case in recursive query)
        Ok(SqlStatement {
            stmt,
            tok: RuleDepRules,
        })
    }

    fn restore_deleted_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(&*format!("DELETE from DeletedList where id = ?"))?;
        Ok(SqlStatement {
            stmt,
            tok: RestoreDeleted,
        })
    }

    fn mark_deleted_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(&*format!("Insert into DeletedList (id) VALUES(?)"))?;
        Ok(SqlStatement {
            stmt,
            tok: RestoreDeleted,
        })
    }
    // get the files written or read by a process (pid) in the latest generation. Latest gen is the one with the highest generation value
    // Latest gen filtering happens after the query results are returned
    fn fetch_io_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT path, typ, gen from DYNIO where pid = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: FetchIO,
        })
    }

    // Envs that this rule depends. The `Export` keyword in Tupfile makes the envs available to the rule
    fn fetch_envs_for_rule_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT name from ENV where id in (SELECT from_id from NormalLink where to_id = ?)")?;
        Ok(SqlStatement {
            stmt,
            tok: FetchEnvsForRule,
        })
    }

    fn fetch_outputs_for_rule_prepare(&self) -> Result<SqlStatement> {
        let typ = RowType::GenF as u8;
        let stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, Node.type type,\
         (DirPathBuf.name || '/' || Node.name) name from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
         where  Node.id in \
        (SELECT to_id from NormalLink where from_id  = ? and to_type={typ}"))?;
        Ok(SqlStatement {
            stmt,
            tok: FetchOutputsForRule,
        })
    }

    fn fetch_inputs_for_rule_prepare(&self) -> Result<SqlStatement> {
        let typ = RowType::File as u8;
        let stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, Node.type type, \
        (DirPathBuf.name || '/' || Node.name) name from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
         where Node.type = {typ} and Node.id in \
        (SELECT from_id from NormalLink where to_id  = ?"))?;
        Ok(SqlStatement {
            stmt,
            tok: FetchInputsForRule,
        })
    }
}

impl LibSqlExec for SqlStatement<'_> {
    fn add_to_modify_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        eyre::ensure!(self.tok == AddToMod, "wrong token for update to modifylist");
        // statement ignores input if already present. So we call the method execute here rather than insert
        self.stmt.execute((id, (rtype as u8)))?;
        Ok(())
    }
    fn add_to_delete_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        eyre::ensure!(self.tok == AddToDel, "wrong token for update to deletelist");
        // statement ignores input if already present. So we call the method execute here rather than insert
        self.stmt.execute((id, (rtype as u8)))?;
        Ok(())
    }
    fn add_to_present_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        eyre::ensure!(
            self.tok == AddToPres,
            "wrong token for update to presentlist"
        );
        self.stmt.execute((id, (rtype as u8)))?;
        Ok(())
    }

    fn add_to_temp_ids_table_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        eyre::ensure!(
            self.tok == AddToTempIds,
            "wrong token for update to tempidslist"
        );
        self.stmt.insert((id, (rtype as u8)))?;
        Ok(())
    }

    fn remove_presents_exec(&mut self) -> Result<()> {
        eyre::ensure!(self.tok == RemovePresents, "wrong token for removing presents");
        self.stmt.execute([])?;
        Ok(())
    }

    fn insert_dir_exec(&mut self, path_str: &str, dir: i64) -> Result<i64> {
        eyre::ensure!(self.tok == InsertDir, "wrong token for Insert dir");
        let id = self.stmt.insert([
            path_str,
            dir.to_string().as_str(),
            (RowType::Dir as u8).to_string().as_str(),
        ])?;
        Ok(id)
    }

    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()> {
        eyre::ensure!(
            self.tok == InsertDirAux,
            "wrong token for Insert Dir Into DirPathBuf"
        );
        let path_str = db_path_str(path);
        self.stmt.insert((id, path_str))?;
        Ok(())
    }

    fn insert_link(&mut self, from_id: i64, to_id: i64, is_sticky: bool, to_type: RowType) -> Result<()> {
        eyre::ensure!(self.tok == InsertLink, "wrong token for insert link");
        debug!("Normal link: {} -> {}", from_id, to_id);
        self.stmt.execute((from_id, to_id, is_sticky as u8, to_type as u8))?;
        Ok(())
    }


    fn insert_node_exec(&mut self, n: &Node) -> Result<i64> {
        eyre::ensure!(self.tok == InsertFile, "wrong token for Insert file");
        let r = self.stmt.insert((
            n.pid,
            n.name.as_str(),
            n.mtime,
            (*n.get_type() as u8),
            n.get_display_str(),
            n.get_flags(),
            n.get_srcid(),
        ))?;
        Ok(r)
    }

    fn insert_env_exec(&mut self, env: &str, val: &str)  -> Result<i64> {
        eyre::ensure!(self.tok == InsertEnv, "wrong token for Insert env");
        let r = self.stmt.insert( (env,val))?;
        Ok(r)
    }

    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64> {
        eyre::ensure!(self.tok == FindDirId, "wrong token for find dir");
        let path_str = db_path_str(p);
        let path_str = path_str.as_str();
        debug!("find dir id for :{}", path_str);
        let id = self.stmt.query_row([path_str], |r| r.get(0))?;
        Ok(id)
    }

    fn fetch_group_id(&mut self, node_name: &str, dir: i64) -> Result<i64> {
        eyre::ensure!(self.tok == FindGroupId, "wrong token for fetch groupid");
        debug!("find group id for :{} at dir:{} ", node_name, dir);
        let v = self.stmt.query_row((dir, node_name), |r| {
            let v: i64 = r.get(0)?;
            Ok(v)
        })?;
        Ok(v)
    }

    fn fetch_node(&mut self, node_name: &str, dir: i64) -> Result<Node> {
        eyre::ensure!(self.tok == FindNode, "wrong token for fetch node");
        log::info!("query for node:{:?}, {:?}", node_name, dir);

        let node = self.stmt.query_row((dir, node_name), make_node)?;
        Ok(node)
    }

    fn fetch_node_by_id(&mut self, i: i64) -> Result<Node> {
        eyre::ensure!(self.tok == FindNodeById, "wrong token for fetch node by id");
        let node = self.stmt.query_row([i], make_node)?;
        Ok(node)
    }

    fn fetch_node_id(&mut self, node_name: &str, dir: i64) -> Result<i64> {
        eyre::ensure!(self.tok == FindNodeId, "wrong token for fetch node");
        log::debug!("query:{:?}, {:?}", node_name, dir);
        let nodeid = self.stmt.query_row((dir, node_name), |r| (r.get(0)))?;
        Ok(nodeid)
    }

    fn fetch_env_id(&mut self, env_name: &str) -> Result<(i64, String)> {
        eyre::ensure!(self.tok == FindEnvId, "wrong token for fetch env");
        log::debug!("query env:{:?}", env_name);
        let (nodeid, val) = self.stmt.query_row((env_name,), |r|
                Ok((r.get(0)?, r.get(1)?))
        )?;
        Ok((nodeid, val))
    }

    fn fetch_rule_nodes_by_dirid(&mut self, dir: i64) -> Result<Vec<Node>> {
        eyre::ensure!(self.tok == FindNodes, "wrong token for fetch nodes");
        let mut rows = self.stmt.query([dir])?;
        let mut nodes = Vec::new();
        while let Some(row) = rows.next()? {
            let node = make_node(row)?;
            nodes.push(node);
        }
        Ok(nodes)
    }

    fn fetch_glob_nodes<F, P>(
        &mut self,
        base_path: P,
        gname: P,
        mut f: F,
    ) -> Result<Vec<MatchingPath>>
        where
            F: FnMut(&String) -> Option<MatchingPath>,
            P: AsRef<Path>,
    {
        eyre::ensure!(
            self.tok == FindGlobNodes,
            "wrong token for fetch glob nodes"
        );
        let path_str = db_path_str(base_path);
        use regex::Regex;
        let gname_str = gname.as_ref().to_string_lossy().replace('*', "%");
        let re = Regex::new(r"\[.*?\]").unwrap();
        let gname_str = re.replace_all(&gname_str, "%");
        debug!("query glob:{:?}", gname_str);
        let mut rows = self.stmt.query((gname_str.into_owned(), path_str))?;
        let mut nodes = Vec::new();
        while let Some(row) = rows.next()? {
            let node: String = row.get(0)?;
            if let Some(id) = f(&node) {
                nodes.push(id);
            }
        }
        Ok(nodes)
    }

    fn fetch_parent_rule(&mut self, id: i64) -> Result<Vec<i64>> {
        eyre::ensure!(
            self.tok == FindParentRule,
            "wrong token for fetch parent rule"
        );
        let mut ids = Vec::new();
        let mut rows = self.stmt.query([id, id])?;
        while let Ok(Some(r)) = rows.next() {
            let id: i64 = r.get(0)?;
            ids.push(id);
        }
        Ok(ids)
    }

    fn fetch_node_path(&mut self, name: &str, dirid: i64) -> Result<PathBuf> {
        eyre::ensure!(self.tok == FindNodePath, "wrong token for fetch node path");
        let path_str: String = self.stmt.query_row([dirid], |r| r.get(0))?;
        Ok(Path::new(path_str.as_str()).join(name))
    }

    fn update_mtime_exec(&mut self, id: i64, mtime_ns: i64) -> Result<()> {
        eyre::ensure!(self.tok == UpdMTime, "wrong token for update mtime");
        self.stmt.execute([id, mtime_ns])?;
        Ok(())
    }

    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()> {
        eyre::ensure!(self.tok == UpdDirId, "wrong token for dirid update");
        self.stmt.execute([dirid, id])?;
        Ok(())
    }
    fn update_env_exec(&mut self, id: i64, val: String) -> Result<()> {
        eyre::ensure!(self.tok == UpdEnv, "wrong token for env display_str update");
        self.stmt.execute((id, val.as_str()))?;
        Ok(())
    }

    fn delete_exec(&mut self, id: i64) -> Result<()> {
        eyre::ensure!(self.tok == DeleteId, "wrong token for delete node");
        self.stmt.execute([id])?;
        Ok(())
    }

    fn mark_rule_outputs_deleted(&mut self, rule_id: i64) -> Result<()> {
        eyre::ensure!(
            self.tok == ImmediateDeps,
            "wrong token for mark immediate rule as deleted"
        );
        self.stmt.execute([rule_id])?;
        Ok(())
    }
    fn fetch_dependant_tupfiles(&mut self, rule_id: i64) -> Result<Vec<Node>> {
        eyre::ensure!(
            self.tok == RuleDepRules,
            "wrong token for delete rule links"
        );
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let n = make_tup_node(r)?;
            vs.push(n);
        }
        Ok(vs)
    }

    fn delete_normal_rule_links(&mut self, rule_id: i64) -> Result<()> {
        eyre::ensure!(
            self.tok == DeleteNormalRuleLinks,
            "wrong token for delete rule links"
        );
        self.stmt.execute([rule_id, rule_id])?;
        Ok(())
    }
    fn find_node_by_path<P: AsRef<Path>>(&mut self, dir_path: P, name: &str) -> Result<(i64, i64)> {
        eyre::ensure!(self.tok == FindNodeByPath, "wrong token for find by path");
        let dp = db_path_str(dir_path);
        let (id, pid) = self.stmt.query_row(
            [name, dp.as_str()],
            |r: &rusqlite::Row| -> Result<(i64, i64), rusqlite::Error> {
                let id: i64 = r.get(0)?;
                let pid: i64 = r.get(1)?;
                Ok((id, pid))
            },
        )?;
        Ok((id, pid))
    }


    fn fetch_io(&mut self, proc_id: i32) -> Result<Vec<(String, u8)>> {
        eyre::ensure!(self.tok == FetchIO, "wrong token for fetchio");
        let mut rows = self.stmt.query([proc_id])?;
        let mut vs = Vec::new();
        let mut lgen = 0;
        while let Some(r) = rows.next()? {
            let s: String = r.get(0)?;
            let i: u8 = r.get(1)?;
            let gen: u32 = r.get(2)?;
            if lgen != gen {
                //  keep only the latest generation's io
                vs.clear();
            }
            vs.push((s, i));
            lgen = gen;
        }
        Ok(vs)
    }

    fn fetch_envs(&mut self, rule_id: i32) -> Result<Vec<String>> {
        eyre::ensure!(self.tok == FetchEnvsForRule, "wrong token for fetchenvs");
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let s: String = r.get(0)?;
            vs.push(s);
        }
        Ok(vs)
    }

    fn fetch_inputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        eyre::ensure!(self.tok == FetchInputsForRule, "wrong token for FetchInputsForRule");
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let n = make_node(r)?;
            vs.push(n);
        }
        Ok(vs)
    }

    fn fetch_outputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        eyre::ensure!(self.tok == FetchOutputsForRule, "wrong token for FetchOutputsForRule");
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let n = make_node(r)?;
            vs.push(n);
        }
        Ok(vs)
    }
}

fn db_path_str<P: AsRef<Path>>(p: P) -> String {
    let input_path_str = p.as_ref().to_string_lossy();
    static UNIX_SEP: char = '/';
    static UNIX_SEP_STR: &str = "/";
    static CD_UNIX_SEP_STR: &str = "./";
    let mut path_str = if MAIN_SEPARATOR != UNIX_SEP {
        input_path_str.into_owned().replace(std::path::MAIN_SEPARATOR_STR, UNIX_SEP_STR)
    } else {
        input_path_str.to_string()
    };
    if !path_str.starts_with(".") && !path_str.starts_with(CD_UNIX_SEP_STR) {
        path_str.insert_str(0, CD_UNIX_SEP_STR)
    }
    path_str
        .strip_suffix(UNIX_SEP_STR)
        .unwrap_or(path_str.as_str())
        .to_string()
}

impl SqlStatement<'_> {}

impl ForEachClauses for Connection {
    fn for_each_file_node_id<F>(&self, f: F) -> Result<()>
        where
            F: FnMut(i64) -> Result<()>,
    {
        let mut stmt = self.prepare("SELECT id from Node where type={} or type={}")?;
        let mut rows = stmt.query([RowType::File as u8, RowType::Dir as u8])?;
        let mut mut_f = f;
        while let Some(row) = rows.next()? {
            let i: i64 = row.get(0)?;
            mut_f(i)?;
        }
        Ok(())
    }
    fn for_each_node_id<P, F>(&self, p: P, f: F) -> Result<()>
    where
        P: Params,
        F: FnMut(i64) -> Result<()>,
    {
        let mut stmt = self.prepare("SELECT id from Node where type=?")?;
        let mut rows = stmt.query(p)?;
        let mut mut_f = f;
        while let Some(row) = rows.next()? {
            let i: i64 = row.get(0)?;
            mut_f(i)?;
        }
        Ok(())
    }

    fn for_changed_or_created_tup_node_with_path<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>,
    {
        let tuptype = RowType::TupF as u8;
        let dirtype = RowType::Dir as u8;
        let mut stmt = self.prepare(&*format!("SELECT id,dir,name from TUPPATHBUF where id in
        (SELECT id from ModifyList where type = {tuptype}
        UNION
(SELECT id from TupPathBuf where dir in (SELECT to_id from NormalLink where to_type={dirtype} from_id in
(SELECT id from ModifyList where type = {dirtype} UNION (SELECT id from DeleteList where type = {dirtype})))))",
        ))?;
        Self::for_tupid_and_path([], f, &mut stmt)?;
        Ok(())
    }

    fn for_each_grp_nodeid_provider<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(i64) -> Result<()>,
    {
        if let Some(rty) = rtype {
            let rty = rty as u8;
            let mut stmt = self.prepare(&format!(
                "SELECT id from NODE \
         where  type = {rty} and id in\
         (SELECT from_id from NormalLink where to_id = {group_id} "
            ))?;
            Self::for_id([], f, &mut stmt)?;
        } else {
            let mut stmt = self.prepare(&format!(
                "SELECT id from NODE \
         where  id in\
         (SELECT from_id from NormalLink where to_id  = {group_id}",
            ))?;
            Self::for_id([], f, &mut stmt)?;
        }
        Ok(())
    }
    /// This runs a foreach over [Node]'s which link to given `group_id`
    /// note that is returns nodes whose names are path from root extracted from temp table DirPathBuf
    fn for_each_grp_node_provider<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>,
    {
        if let Some(rty) = rtype {
            let typ = rty as u8;
            let mut stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, Node.type type, (DirPathBuf.name || '/' || Node.name) name from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
         where  Node.type = {typ} and Node.id in \
         (SELECT from_id from NormalLink where to_id  = {group_id})"))?;
            Self::for_node([], f, &mut stmt)?;
        } else {
            let mut stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, Node.type type, (DirPathBuf.name || '/' || Node.name) name from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
         where  Node.id in \
        (SELECT from_id from NormalLink where to_id  = {group_id})"))?;
            Self::for_node([], f, &mut stmt)?;
        }
        Ok(())
    }
    fn for_each_grp_node_requirer<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>,
    {
        if let Some(rty) = rtype {
            let rty = rty as u8;
            let mut stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, DirPathBuf.name name from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
          where  Node.type={rty} AND Node.id in \
        (SELECT to_id from normal_link where from_id = {group_id})")
            )?;
            Self::for_node([], f, &mut stmt)?;
        } else {
            let mut stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, DirPathBuf.name name from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
          where  Node.id in \
        (SELECT to_id from normal_link where from_id = {group_id} )")
            )?;
            Self::for_node([], f, &mut stmt)?;
        }
        Ok(())
    }
    fn fetch_db_sticky_inputs(&self, rule_id: i64) -> Result<Vec<i64>> {
        let mut stmt = self.prepare("SELECT from_id from StickyLink where to_id = ?")?;
        Self::rule_link(rule_id, &mut stmt)
    }

    fn fetch_db_inputs(&self, rule_id: i64) -> Result<Vec<i64>> {
        let mut stmt = self.prepare("SELECT from_id from NormalLink where to_id = ?")?;
        Self::rule_link(rule_id, &mut stmt)
    }
    fn fetch_db_outputs(&self, rule_id: i64) -> Result<Vec<i64>> {
        let mut stmt = self.prepare("SELECT to_id from NormalLink where from_id = ?")?;
        Self::rule_link(rule_id, &mut stmt)
    }

    fn for_tupid_and_path<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
    where
        P: Params,
        F: FnMut(Node) -> Result<()>,
    {
        let mut rows = stmt.query(p)?;
        let mut mut_f = f;
        while let Some(row) = rows.next()? {
            mut_f(make_tup_node(row)?)?
        }
        Ok(())
    }
    fn for_id<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
    where
        P: Params,
        F: FnMut(i64) -> Result<()>,
    {
        let mut rows = stmt.query(p)?;
        let mut mut_f = f;
        while let Some(row) = rows.next()? {
            let i = row.get(0)?;
            mut_f(i)?;
        }
        Ok(())
    }

    fn for_node<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
    where
        P: Params,
        F: FnMut(Node) -> Result<()>,
    {
        let mut rows = stmt.query(p)?;
        let mut mut_f = f;
        while let Some(row) = rows.next()? {
            let i = row.get(0)?;
            let dir: i64 = row.get(1)?;
            let rtype: i8 = row.get(2)?;
            let name: String = row.get(3)?;
            let rtype = rtype as u8;
            let rty: RowType = RowType::try_from(rtype).map_err(|e| {
                eyre::Error::msg(format!(
                    "unknown row type returned \
                 in foreach node query :{}",
                    e
                ))
            })?;
            mut_f(Node::new(i, dir, 0, name, rty))?;
        }
        Ok(())
    }

    fn for_each_link<F>(&mut self, f: F) -> Result<()>
        where F: FnMut(i32, i32) -> Result<()>
    {
        let mut stmt = self.prepare("SELECT from_id, to_id from NormalLink")?;
        let mut rows = stmt.query([])?;
        let mut mut_f = f;
        while let Some(row) = rows.next()? {
            let i: i32 = row.get(0)?;
            let j: i32 = row.get(1)?;
            mut_f(i, j)?;
        }
        Ok(())
    }

    fn rule_link(rule_id: i64, stmt: &mut Statement) -> Result<Vec<i64>> {
        let mut inputs = Vec::new();

        let _ = stmt.query_map([rule_id], |r| -> rusqlite::Result<()> {
            let id = r.get(0)?;
            inputs.push(id);
            Ok(())
        })?;
        Ok(inputs)
    }

    fn rules_to_run_no_target(&self) -> Result<Vec<Node>> {
        let rtype = RowType::Rule as u8;
        let stmt = &*format!("SELECT id from ModifyList where type = {rtype}");
        let mut stmt = self.prepare(stmt)?;
        let mut rules = Vec::new();
        let _ = stmt.query_map([], |r| -> rusqlite::Result<()> {
            let n = crate::make_rule_node(r)?;
            rules.push(n);
            Ok(())
        })?;
        Ok(rules)
    }

    fn get_target_ids(&self, root: &Path, targets: &Vec<String>) -> Result<Vec<i64>> {
        let dir = std::env::current_dir().unwrap();
        let dirc = dir.clone();
        let dir = diff_paths(dir, root).ok_or(eyre::Error::msg(format!("Could not get relative path for:{:?}", dirc.as_path())))?;
        let mut fd = self.fetch_dirid_prepare()?;
        let mut fnode_stmt = self.fetch_node_by_id_prepare()?;
        let mut dirids = Vec::new();
        for t in targets {
            let path = dir.join(t.as_str());
            if let Ok(id) = fd.fetch_dirid(dir.join(&targets[0])) {
                dirids.push(id);
            } else {
                // get the parent dir id and fetch the node by its name
                let parent = path.parent().ok_or(eyre::Error::msg(format!("Could not get parent for:{:?}",
                                                                          path)))?;
                if let Some(parent_id) = fd.fetch_dirid(parent).ok() {
                    if let Ok(nodeid) = fnode_stmt.fetch_node_id(&*path.file_name().unwrap().to_string_lossy().to_string(),
                                                                 parent_id)
                    {
                        dirids.push(nodeid);
                    } else {
                        return Err(eyre::Error::msg(format!("Could not find target node id for:{:?}", path)));
                    }
                }
            }
        }
        Ok(dirids)
    }
}

impl MiscStatements for Connection {
    fn populate_delete_list(&self) -> Result<()> {
        let ftype = RowType::File as u8;
        let dtype = RowType::Dir as u8;
        // add all files and directories to delete list that are not in the present list or modify list
        let mut stmt = self.prepare(
            &*format!("Insert  or IGNORE into DeleteList SELECT id, type from Node Where (type= {ftype} or type = {dtype} ) and id NOT in( \
         SELECT id from PresentList Union Select id from ModifyList)",
            ))?;
        stmt.execute([])?;

        Ok(())
    }

    fn enrich_modified_list(&self) -> Result<()> {
// add parent dirs to modify list if children of that directory are already in it
        let ftype = RowType::File as u8;
        let gentype = RowType::GenF as u8;
        let dtype = RowType::Dir as u8;
        let rtype = RowType::Rule as u8;
        let gdtype = RowType::DirGen as u8;
        let grptype = RowType::Grp as u8;
        {
            // add parent dirs to modify list if any child of that directory is in the delete list
            let mut stmt = self.prepare(
                &*format!("Insert or IGNORE into ModifyList  SELECT dir,type from Node where id in \
            (SELECT id from ModifyList where type = {ftype} or type = {gentype} UNION \
            (SELECT id from DeleteList where type = {ftype} or type = {gentype}) )"),
            )?;
            stmt.execute([])?;

            // mark groups as Modified if any of its inputs are in the deletelist or modified list
            let mut stmt = self.prepare(
                &*format!("Insert or IGNORE into ModifyList SELECT id, {grptype} from Node where  type = {rtype} and id in \
            (SELECT to_id from NormalLink where from_id in (SELECT id from DeleteList where type = {gentype} UNION SELECT id from ModifyList where type = {gentype} ) )"),
            )?;
            stmt.execute([])?;
            // mark rules as Modified if any of its outputs are in the deletelist
            let mut stmt = self.prepare(
                &*format!("Insert or IGNORE into ModifyList SELECT id, {rtype} from Node where  type = {rtype} and id in \
            (SELECT from_id from NormalLink where to_id in (SELECT id from DeleteList  UNION SELECT id from ModifyList ) )"),
            )?;
            stmt.execute([])?;
            // mark rules as Modified if any of its inputs are in the deletelist
            let mut stmt = self.prepare(
                &*format!("Insert or IGNORE into ModifyList SELECT id, {rtype} from Node where  type = {rtype} and id in \
            (SELECT to_id from NormalLink where from_id in (SELECT id from DeleteList  UNION SELECT id from ModifyList ) )"),
            )?;
            stmt.execute([])?;
        }

        Ok(())
    }

    fn remove_id_from_delete_list(&self, id: i64) -> Result<()> {
        let mut stmt = self.prepare_cached("DELETE FROM DeleteList WHERE id = ?")?;
        stmt.execute([id])?;
        Ok(())
    }

    fn prune_present_list(&self) -> Result<()> {
        let mut stmt = self.prepare_cached("DELETE FROM PresentList WHERE id in (SELECT id from DeleteList)")?;
        stmt.execute([])?;
        Ok(())
    }

    fn prune_modified_list(&self) -> Result<()> {
        let mut stmt = self.prepare_cached("DELETE FROM ModifyList WHERE id in (SELECT id from DeleteList)")?;
        stmt.execute([])?;
        Ok(())
    }

    fn insert_trace<P: AsRef<Path>>(&mut self, filepath: P, pid: u32, gen: u32, typ: i8, childcnt: i32) -> Result<i64> {
        let mut stmt = self.prepare_cached("INSERT INTO DYNIO (path, pid, gen, typ, childcnt) VALUES (?, ?, ?, ?, ?)")?;
        let filepath = db_path_str(filepath);
        let id = stmt.insert((filepath, pid, gen, typ, childcnt))?;
        Ok(id)
    }

    fn remove_modified_list(&self) -> Result<()> {
        let mut stmt = self.prepare_cached("DELETE FROM ModifyList")?;
        stmt.execute([])?;
        Ok(())
    }
}

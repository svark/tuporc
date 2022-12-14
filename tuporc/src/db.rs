use crate::db::RowType::{Grp, TupF};
use crate::db::StatementType::*;
use crate::make_node;
use crate::RowType::Rule;
use anyhow::Result;
use log::debug;
use rusqlite::{Connection, Params, Statement};
use std::cmp::Ordering;
use std::fs::File;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};

#[derive(Clone, Debug, Copy, PartialEq, Eq, FromPrimitive)]
pub enum RowType {
    File = 0, //< Fite
    Rule = 1, //< Rule
    Dir = 2, //< Directory
    Env = 3, //< Env var
    GenF = 4,//< Generated file
    TupF = 5, //< Tupfile or lua
    Grp = 6, //< Group
    GenD = 7, //< Generated Directory
}

impl ToString for RowType {
    fn to_string(&self) -> String {
        (*self as u8).to_string()
    }
}

/// Fields in the Node table in the database
#[derive(Clone, Debug)]
pub struct Node {
    id: i64,
    pid: i64, //< Parent folder id
    mtime: i64, // time in nano s
    name: String, // name of file or dir or rule or group
    rtype: RowType,// type of data in this row
    display_str: String, // rule display
    flags: String, // flags for rule that appear in description
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
            rtype: Rule,
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
            rtype: Grp,
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
        &self.display_str.as_str()
    }
    pub fn get_flags(&self) -> &str {
        &self.flags.as_str()
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
    InsertDir,
    InsertDirAux,
    InsertFile,
    InsertLink,
    InsertStickyLink,
    FindDirId,
    FindGroupId,
    FindNode,
    FindNodeId,
    FindNodeById,
    FindNodes,
    FindGlobNodes,
    FindNodePath,
    FindParentRule,
    UpdMTime,
    UpdDirId,
    DeleteId,
    DeleteRuleLinks,
}

pub struct SqlStatement<'conn> {
    stmt: Statement<'conn>,
    tok: StatementType,
}

pub(crate) trait LibSqlPrepare {
    fn add_to_modify_prepare(&self) -> Result<SqlStatement>;
    fn add_to_delete_prepare(&self) -> Result<SqlStatement>;
    fn add_to_present_prepare(&self) -> Result<SqlStatement>;
    fn insert_dir_prepare(&self) -> Result<SqlStatement>;
    fn insert_dir_aux_prepare(&self) -> Result<SqlStatement>;
    fn insert_sticky_link_prepare(&self) -> Result<SqlStatement>;
    fn insert_link_prepare(&self) -> Result<SqlStatement>;
    fn insert_node_prepare(&self) -> Result<SqlStatement>;
    fn fetch_dirid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_groupid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_prepare(&self) -> Result<SqlStatement>;
    fn fetch_nodeid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_by_id_prepare(&self) -> Result<SqlStatement>;
    fn fetch_nodes_prepare_by_dirid(&self) -> Result<SqlStatement>;
    fn fetch_glob_nodes_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_path_prepare(&self) -> Result<SqlStatement>;
    fn fetch_parent_rule_prepare(&self) -> Result<SqlStatement>;
    fn update_mtime_prepare(&self) -> Result<SqlStatement>;
    fn update_dirid_prepare(&self) -> Result<SqlStatement>;
    fn delete_prepare(&self) -> Result<SqlStatement>;
    fn delete_tup_rule_links_prepare(&self) -> Result<SqlStatement>;
}

pub(crate) trait LibSqlExec {
    fn add_to_modify_exec(&mut self, id: i64, rtyp: RowType) -> Result<()>;
    fn add_to_delete_exec(&mut self, id: i64, rtype: RowType) -> Result<()>;
    fn add_to_present_exec(&mut self, id: i64, rtype: RowType) -> Result<()>;
    fn insert_dir_exec(&mut self, path_str: &str, dir: i64) -> Result<i64>;
    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()>;
    fn insert_link(&mut self, from_id: i64, to_id: i64) -> Result<()>;
    fn insert_sticky_link(&mut self, from_id: i64, to_id: i64) -> Result<()>;
    fn insert_node_exec(&mut self, n: &Node) -> Result<i64>;
    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64>;
    fn fetch_group_id(&mut self, node_name: &str, dir: i64) -> Result<i64>;
    fn fetch_node(&mut self, node_name: &str, dir: i64) -> Result<Node>;
    fn fetch_node_by_id(&mut self, i: i64) -> Result<Node>;
    fn fetch_node_id(&mut self, node_name: &str, dir: i64) -> Result<i64>;
    fn fetch_nodes_by_dirid<P: Params>(&mut self, params: P) -> Result<Vec<Node>>;
    fn fetch_parent_rule(&mut self, id: i64) -> Result<i64>;
    fn fetch_node_path(&mut self, name: &str, dirid: i64) -> Result<PathBuf>;
    fn update_mtime_exec(&mut self, dirid: i64, mtime_ns: i64) -> Result<()>;
    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()>;
    fn delete_exec(&mut self, id: i64) -> Result<()>;
    fn delete_rule_links(&mut self, rule_id: i64) -> Result<()>;
}
pub(crate) trait MiscStatements {
    fn populate_delete_list(&self) -> Result<()>;
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
    //fn populate_delete_list(&self) -> Result<()>;
    fn rules_to_run(&self) -> Result<Vec<Node>>;
}

// Check if the node table exists in .tup/db
pub fn is_initialized(conn: &Connection) -> bool {
    if let Ok(mut stmt) =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?; ")
    {
        stmt.query_row(["Node"], |_x| Ok(true)).is_ok()
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
    conn.execute(
        "CREATE TABLE Node(id  INTEGER PRIMARY KEY not NULL, dir INTEGER not NULL, type INTEGER not NULL,\
                         name VARCHAR(4096),  mtime_ns INTEGER DEFAULT 0, display_str VARCHAR(4096), flags VARCHAR(256), srcid INTEGER default -1, unique(dir, name));",
        (),
    )
        .expect("Node table already exists.");

    // NormalLink has links between tup files/rules/groups etc
    conn.execute(
        "CREATE TABLE NormalLink (from_id INTEGER, to_id INTEGER, \
          unique (from_id, to_id) ); ",
        (),
    )
    .expect("Failed to create NormalLink Table");

    // Above table is indexed based on to_id
    conn.execute("CREATE INDEX NormalLinkIdx ON NormalLink(to_id);", ())
        .expect("Failed to create index on NormalLink Table");

    // StickyLinks are static links
    conn.execute(
        "CREATE TABLE StickyLink (from_id INTEGER, to_id INTEGER,\
      unique (from_id, to_id)); ",
        (),
    )
    .expect("Failed to create StickyLink Table");

    // Var table has env variables
    conn.execute(
        "CREATE TABLE Var ( id INTEGER PRIMARY KEY, value VARCHAR);",
        (),
    )
    .expect("Failed to create Var table");
    let _ = File::create("Tupfile.ini").expect("could not open Tupfile.ini for write");
    println!("Finished creating tables");
}

// create a temp table from directories paths to their node ids
pub fn create_dir_path_buf_temptable(conn: &Connection) -> Result<()> {
    // https://gist.github.com/jbrown123/b65004fd4e8327748b650c77383bf553
    let stmt = format!(
        "DROP TABLE IF EXISTS DIRPATHBUF;
CREATE TEMPORARY TABLE DIRPATHBUF AS WITH RECURSIVE full_path(id, name) AS
(
    VALUES(1, '.')
    UNION ALL
	SELECT  node.id id, full_path.name || '/' || node.name name
           FROM node JOIN full_path ON node.dir=full_path.id
            where node.type={}
 ) SELECT  id, name from full_path",
        RowType::Dir as u8
    );
    conn.execute_batch(stmt.as_str())?;
    Ok(())
}

// creates a temp table for groups
pub fn create_group_path_buf_temptable(conn: &Connection) -> Result<()> {
    let stmt = format!(
        "
    DROP TABLE IF EXISTS GRPPATHBUF;
CREATE TEMPORARY TABLE GRPPATHBUF AS
   SELECT  node.id id ,DIRPATHBUF.name || '/' || node.name Name from node inner join DIRPATHBUF on
       (node.dir=DIRPATHBUF.id and node.type={})",
        RowType::Grp as u8
    );
    conn.execute_batch(stmt.as_str())?;
    Ok(())
}

//creates a temp table for tup file paths
pub fn create_tup_path_buf_temptable(conn: &Connection) -> Result<()> {
    let stmt = format!("
DROP TABLE IF EXISTS TUPPATHBUF;
CREATE TEMPORARY TABLE TUPPATHBUF AS
SELECT node.id id, node.dir dir, node.mtime_ns mtime, DIRPATHBUF.name || '/' || node.name name from Node inner join DIRPATHBUF ON
(NODE.dir = DIRPATHBUF.id and node.type={})", TupF as u8);
    conn.execute_batch(stmt.as_str())?;
    Ok(())
}
/*pub fn create_rule_buf_temptable(conn: &Connection) -> Result<()> {
    let stmt = format!("
DROP TABLE IF EXISTS RULEBUF;
CREATE TEMPORARY TABLE RULEBUF AS
SELECT node.id id, node.dir dir, node.mtime_ns mtime, DIRPATHBUF.name name from Node inner join DIRPATHBUF ON
(NODE.dir = DIRPATHBUF.id and node.type={})", Rule as u8);
    conn.execute_batch(stmt.as_str())?;
    Ok(())
}*/

pub fn create_temptables(conn: &Connection) -> Result<()> {
    let stmt = "DROP TABLE IF EXISTS PresentList; CREATE TABLE PresentList(id  INTEGER PRIMARY KEY not NULL, type INTEGER); \
    Drop Table If Exists ModifyList; Create Table ModifyList(id INTEGER PRIMARY KEY not NULL, type INTEGER);\
    Drop Table If Exists DeleteList; Create Table DeleteList(id INTEGER PRIMARY KEY not NULL, type INTEGER);\
";
    conn.execute_batch(stmt)?;
    Ok(())
}

impl LibSqlPrepare for Connection {
    fn add_to_modify_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into ModifyList(id, type) Values (?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: AddToMod,
        })
    }
    fn add_to_delete_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into DeleteList(id, type) Values (?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: AddToDel,
        })
    }
    fn add_to_present_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into PresentList(id, type) Values(?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: AddToPres,
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

    fn insert_sticky_link_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into StickyLink (from_id, to_id) Values (?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertStickyLink,
        })
    }
    fn insert_link_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into NormalLink (from_id, to_id) Values (?,?)")?;
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

    fn fetch_nodes_prepare_by_dirid(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT id, dir, mtime_ns, name, type FROM Node where dir=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodes,
        })
    }

    fn fetch_glob_nodes_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(
            "SELECT id, dir, mtime_ns, name, type FROM Node where dir=? and name LIKE ?",
        )?;
        Ok(SqlStatement {
            stmt,
            tok: FindGlobNodes,
        })
    }

    fn fetch_node_path_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT fullpath from DirPathBuf where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodePath,
        })
    }

    fn fetch_parent_rule_prepare(&self) -> Result<SqlStatement> {
        let rty = Rule as u8;
        let stmtstr = format!(
            "SELECT id FROM Node where type={rty} and id in \
        (SELECT from_id from NormalLink where to_id = ?)"
        );
        let stmt = self.prepare(stmtstr.as_str())?;
        Ok(SqlStatement {
            stmt,
            tok: FindParentRule,
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

    fn delete_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE FROM Node WHERE id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: DeleteId,
        })
    }


    fn delete_tup_rule_links_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(
            "delete from StickyLink where from_id = ? or to_id  = ?;\
            delete from NodeLink where from_id = ? or to_id  = ?",
        )?;
        Ok(SqlStatement {
            stmt,
            tok: DeleteRuleLinks,
        })
    }
}

impl LibSqlExec for SqlStatement<'_> {
    fn add_to_modify_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        anyhow::ensure!(self.tok == AddToMod, "wrong token for update to modifylist");
        self.stmt.insert((id, (rtype as u8)))?;
        Ok(())
    }
    fn add_to_delete_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        anyhow::ensure!(self.tok == AddToDel, "wrong token for update to deletelist");
        self.stmt.insert((id, (rtype as u8)))?;
        Ok(())
    }
    fn add_to_present_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        anyhow::ensure!(
            self.tok == AddToPres,
            "wrong token for update to presentlist"
        );
        self.stmt.insert((id, (rtype as u8)))?;
        Ok(())
    }

    fn insert_dir_exec(&mut self, path_str: &str, dir: i64) -> Result<i64> {
        anyhow::ensure!(self.tok == InsertDir, "wrong token for Insert dir");
        let id = self.stmt.insert([
            path_str,
            dir.to_string().as_str(),
            (RowType::Dir as u8).to_string().as_str(),
        ])?;
        Ok(id)
    }

    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()> {
        anyhow::ensure!(
            self.tok == InsertDirAux,
            "wrong token for Insert Dir Into DirPathBuf"
        );
        let path_str = SqlStatement::db_path_str(path);
        self.stmt.insert((id, path_str))?;
        Ok(())
    }

    fn insert_link(&mut self, from_id: i64, to_id: i64) -> Result<()> {
        anyhow::ensure!(self.tok == InsertLink, "wrong token for insert link");
        self.stmt.insert([from_id, to_id])?;
        Ok(())
    }

    fn insert_sticky_link(&mut self, from_id: i64, to_id: i64) -> Result<()> {
        anyhow::ensure!(
            self.tok == InsertStickyLink,
            "wrong token for insert sticky link"
        );
        self.stmt.insert([from_id, to_id])?;
        Ok(())
    }

    fn insert_node_exec(&mut self, n: &Node) -> Result<i64> {
        anyhow::ensure!(self.tok == InsertFile, "wrong token for Insert file");
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

    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64> {
        anyhow::ensure!(self.tok == FindDirId, "wrong token for find dir");
        let path_str = Self::db_path_str(p);
        let path_str = path_str.as_str();
        debug!("find dir id for :{}", path_str);
        let id = self.stmt.query_row([path_str], |r| r.get(0))?;
        Ok(id)
    }

    fn fetch_group_id(&mut self, node_name: &str, dir: i64) -> Result<i64> {
        anyhow::ensure!(self.tok == FindGroupId, "wrong token for fetch groupid");
        debug!("find group id for :{} at dir:{} ", node_name, dir);
        let v = self.stmt.query_row((dir, node_name), |r| {
            let v: i64 = r.get(0)?;
            Ok(v)
        })?;
        Ok(v)
    }

    fn fetch_node(&mut self, node_name: &str, dir: i64) -> Result<Node> {
        anyhow::ensure!(self.tok == FindNode, "wrong token for fetch node");
        log::info!("query for node:{:?}, {:?}", node_name, dir);

        let node = self.stmt.query_row((dir, node_name), make_node)?;
        Ok(node)
    }

    fn fetch_node_by_id(&mut self, i: i64) -> Result<Node> {
        anyhow::ensure!(self.tok == FindNodeById, "wrong token for fetch node by id");
        let node = self.stmt.query_row([i], make_node)?;
        Ok(node)
    }

    fn fetch_node_id(&mut self, node_name: &str, dir: i64) -> Result<i64> {
        anyhow::ensure!(self.tok == FindNodeId, "wrong token for fetch node");
        log::debug!("query:{:?}, {:?}", node_name, dir);
        let nodeid = self.stmt.query_row((dir, node_name), |r| (r.get(0)))?;
        Ok(nodeid)
    }

    fn fetch_nodes_by_dirid<P: Params>(&mut self, params: P) -> Result<Vec<Node>> {
        anyhow::ensure!(self.tok == FindNodes, "wrong token for fetch nodes");
        let mut rows = self.stmt.query(params)?;
        let mut nodes = Vec::new();
        while let Some(row) = rows.next()? {
            let node = make_node(row)?;
            nodes.push(node);
        }
        Ok(nodes)
    }

    fn fetch_parent_rule(&mut self, id: i64) -> Result<i64> {
        anyhow::ensure!(
            self.tok == FindParentRule,
            "wrong token for fetch parent rule"
        );
        let nodeid = self.stmt.query_row([id], |r| (r.get(0)))?;
        Ok(nodeid)
    }

    fn fetch_node_path(&mut self, name: &str, dirid: i64) -> Result<PathBuf> {
        anyhow::ensure!(self.tok == FindNodePath, "wrong token for fetch node path");
        let path_str: String = self.stmt.query_row([dirid], |r| r.get(0))?;
        Ok(Path::new(path_str.as_str()).join(name))
    }

    fn update_mtime_exec(&mut self, id: i64, mtime_ns: i64) -> Result<()> {
        anyhow::ensure!(self.tok == UpdMTime, "wrong token for update mtime");
        self.stmt.execute([id, mtime_ns])?;
        Ok(())
    }

    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()> {
        anyhow::ensure!(self.tok == UpdDirId, "wrong token for dirid update");
        self.stmt.execute([dirid, id])?;
        Ok(())
    }

    fn delete_exec(&mut self, id: i64) -> Result<()> {
        anyhow::ensure!(self.tok == DeleteId, "wrong token for delete node");
        self.stmt.execute([id])?;
        Ok(())
    }

    fn delete_rule_links(&mut self, rule_id: i64) -> Result<()> {
        anyhow::ensure!(
            self.tok == DeleteRuleLinks,
            "wrong token for delete rule links"
        );
        self.stmt.execute([rule_id, rule_id, rule_id, rule_id])?;
        Ok(())
    }
}

impl SqlStatement<'_> {
    fn db_path_str<P: AsRef<Path>>(p: P) -> String {
        let input_path_str = p.as_ref().to_string_lossy().to_string();
        static UNIX_SEP: char = '/';
        static UNIX_SEP_STR: &str = "/";
        static CD_UNIX_SEP_STR: &str = "./";
        let mut path_str = if MAIN_SEPARATOR != UNIX_SEP {
            input_path_str.replace(MAIN_SEPARATOR.to_string().as_str(), UNIX_SEP_STR)
        } else {
            input_path_str
        };
        if path_str.as_str() != "." && !path_str.starts_with(CD_UNIX_SEP_STR) {
            path_str.insert_str(0, CD_UNIX_SEP_STR)
        }
        path_str
            .strip_suffix(UNIX_SEP_STR)
            .unwrap_or(path_str.as_str())
            .to_string()
    }
}

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
        let mut stmt = self.prepare("SELECT id,dir,name from TUPPATHBUF")?;
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
                "SELECT Node.id id from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
         where  Node.type = {rty} and Node.id in\
         (SELECT from_id from NormalLink where to_id = {group_id} "
            ))?;
            Self::for_id([], f, &mut stmt)?;
        } else {
            let mut stmt = self.prepare(&format!(
                "SELECT Node.id id from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
         where  Node.id in\
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
            let i = row.get(0)?;
            let dir: i64 = row.get(1)?;
            let name: String = row.get(2)?;
            mut_f(Node::new(i, dir, 0, name, TupF))?;
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
            let rtype: i64 = row.get(2)?;
            let name: String = row.get(3)?;
            let rty: RowType = num::FromPrimitive::from_i64(rtype).ok_or_else(|| {
                anyhow::Error::msg(
                    "unknown row type returned \
                 in foreach node query",
                )
            })?;
            mut_f(Node::new(i, dir, 0, name, rty))?;
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

    fn rules_to_run(&self) -> Result<Vec<Node>> {
        let stmt = "DROP TABLE IF EXISTS ModLink;
        Create TEMPORARY TABLE ModLink as
        SELECT NormalLink.to_id to_id  from NormalLink inner join ModifyList on ModifyList.id = NormalLink.from_id;
        select Node.id, Node.dir, Node.name, Node.display_str, Node.flags from Node INNER join ModLink on (Node.id = ModLink.to_id) and Node.type = ?";
        let mut stmt = self.prepare(stmt)?;
        let mut rules = Vec::new();
        let _ = stmt.query_map([Rule as u8], |r| -> rusqlite::Result<()> {
            let id = crate::make_rule_node(r)?;
            rules.push(id);
            Ok(())
        })?;
        Ok(rules)
    }
}

impl MiscStatements for Connection {
    fn populate_delete_list(&self) -> Result<()> {
        let mut stmt = self.prepare(
            "Insert into DeleteList SELECT id, type from Node Except \
         SELECT id, type from PresentList Union Select id, type from ModifyList",
        )?;
        stmt.execute([])?;
        Ok(())
    }
}

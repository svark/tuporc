use crate::db::StatementType::{AddToMod, DeleteId, DeleteIdAux, FindDirId, FindNode, FindNodes, InsertDir, InsertDirAux, InsertFile, InsertLink, InsertMod, InsertStickyLink, UpdDirId, UpdMTime};
use crate::make_node;
use crate::RowType::{RuleType, TupFType};
use anyhow::Result;
use rusqlite::{Connection, Params, Row, Statement};
use std::collections::HashMap;
use std::fs::File;
use std::path::Path;

#[derive(Clone, Debug)]
pub(crate) enum RowType {
    FileType = 0,
    RuleType = 1,
    DirType = 2,
    EnvType = 3,
    GEnFType = 4,
    TupFType = 5,
    GrpType = 6,
    GEndType = 7,
}

/// Fields in the Node table
#[derive(Clone, Debug)]
pub(crate) struct Node {
    id: i64,
    pid: i64,
    mtime: i64,
    name: String,
    rtype: RowType,
}
impl Node {
    pub fn new(id: i64, pid: i64, mtime: i64, name: String, rtype: RowType) -> Node {
        Node {
            id,
            pid,
            mtime,
            name,
            rtype,
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
}
pub enum StatementType {
    AddToMod,
    InsertDir,
    InsertDirAux,
    InsertFile,
    InsertLink,
    InsertStickyLink,
    FindDirId,
    FindNode,
    FindNodes,
    UpdMTime,
    UpdDirId,
    InsertMod,
    DeleteId,
    DeleteIdAux,
}

pub struct SqlStatement<'conn> {
    stmt: Statement<'conn>,
    tok: StatementType,
}
pub(crate) trait LibSqlPrepare {
    fn add_to_modify_prepare(&self) -> Result<SqlStatement>;
    fn insert_dir_prepare(&self) -> Result<SqlStatement>;
    fn insert_dir_aux_prepare(&self) -> Result<SqlStatement>;
    fn insert_sticky_link_prepare(&self) -> Result<SqlStatement>;
    fn insert_link_prepare(&self) -> Result<SqlStatement>;
    fn insert_node_prepare(&self) -> Result<SqlStatement>;
    fn find_dirid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_prepare(&self) -> Result<SqlStatement>;
    fn fetch_nodes_prepare(&self) -> Result<SqlStatement>;
    fn update_mtime_prepare(&self) -> Result<SqlStatement>;
    fn update_dirid_prepare(&self) -> Result<SqlStatement>;
    fn delete_prepare(&self) -> Result<SqlStatement>;
    fn delete_aux_prepare(&self) -> Result<SqlStatement>;
}

pub(crate) trait LibSqlExec {
    fn add_to_modify_exec(&mut self, id: i64) -> Result<()>;
    fn insert_dir_exec(&mut self, path_str: &str) -> Result<i64>;
    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, pbuf: P) -> Result<()>;
    fn insert_link(&mut self, from_id: i64, to_id: i64) -> Result<()>;
    fn insert_sticky_link(&mut self, from_id: i64, to_id: i64) -> Result<()>;
    fn insert_node_exec(&mut self, n: Node, existing: &[Node]) -> Result<i64>;
    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64>;
    fn fetch_node<P: Params>(&mut self, p: P) -> Result<Node>;
    fn fetch_nodes<P: Params>(&mut self, params: P) -> Result<HashMap<i64, Node>>;
    fn update_mtime_exec(&mut self, dirid: i64, mtime_ns: i64) -> Result<()>;
    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()>;
    fn delete_exec(&mut self, id: i64) -> Result<()>;
    fn delete_exec_aux(&mut self, id: i64) -> Result<()>;
}

pub(crate) trait ForEachClauses {
    fn for_each_fnode_id<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(i64) -> rusqlite::Result<()>;
    fn for_each_node_id<P, F>(&self, p: P, f: F) -> Result<()>
    where
        P: Params,
        F: FnMut(i64) -> rusqlite::Result<()>;
    fn for_each_tupnode_with_path<F>(&self, f: F) -> Result<()>
    where
        P: Params,
        F: FnMut(i64, &Path) -> rusqlite::Result<()>;
    fn for_each_grpnode_provider<F>(&self, name: &str, f: F) -> Result<()>
    where
        F: FnMut(i64, &Path) -> rusqlite::Result<()>;
    fn for_each_grpnode_requirer<F>(&self, name: &str, f: F) -> Result<()>
    where
        F: FnMut(i64, &Path) -> rusqlite::Result<()>;
    fn fetch_db_sticky_inputs(&self, rule_id: i64) -> Result<Vec<i64>>;
    fn fetch_db_inputs(&self, rule_id: i64) -> Result<Vec<i64>>;
    fn fetch_db_outputs(&self, rule_id: i64) -> Result<Vec<i64>>;
    fn delete_db_rule_outputs(&self, rule_id: i64) -> Result<()>;
    fn delete_db_rule_inputs(&self, rule_id: i64) -> Result<()>;

    fn fetch_db_rules(&self, tupfile_id: i64) -> Result<Vec<Node>>;
    fn for_id_and_path<P, F>(p: P, f: F, stmt: &mut Statement) -> rusqlite::Result<()>
    where
        P: Params,
        F: FnMut(i64, &Path) -> rusqlite::Result<()>;
    fn rule_link(rule_id: i64, stmt: &mut Statement) -> Result<Vec<i64>>;
}

// Check is the node table exists in .tup/db
pub fn is_initialized(conn: &Connection) -> bool {
    if let Ok(mut stmt) =
        conn.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name=?; ")
    {
        stmt.query_row(["Node"], |_x| Ok(true)).is_ok()
    } else {
        false
    }
}

// handle the tup init subcommand. This creates the file .tup\db and adds the tables
pub fn init_db() {
    println!("Creating a new db.");
    //use std::fs;
    std::fs::create_dir_all(".tup").expect("Unable to access .tup dir");
    let conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
    conn.execute(
        "CREATE TABLE Node(id  INTEGER PRIMARY KEY, dir INTEGER, type INTEGER,\
                         name TEXT NOT NULL, display VARCHAR(16), mtime_ns INTEGER);",
        (),
    )
    .expect("Failed to create Node Table");
    conn.execute(
        "CREATE TABLE NodeLink (from_id INTEGER, to_id INTEGER );",
        (),
    )
    .expect("Failed to create NodeLink Table");
    conn.execute("CREATE INDEX NodeLink_From ON NodeLink(from_id);", ())
        .expect("Failed to create index  NodeLink_from Table");
    conn.execute("CREATE INDEX NodeLink_To ON NodeLink(to_id);", ())
        .expect("Failed to create index on NodeLinkTable");
    conn.execute("CREATE TABLE ModifyList (id INTEGER PRIMARY KEY); ", ())
        .expect("Failed to create ModifyList Table");
    conn.execute("CREATE TABLE StickyLink (id INTEGER PRIMARY KEY); ", ())
        .expect("Failed to create StickyLink Table");
    conn.execute("CREATE TABLE DeleteList (id INTEGER PRIMARY KEY); ", ())
        .expect("Failed to create DeleteList Table");
    conn.execute(
        "CREATE TABLE Var ( id INTEGER PRIMARY KEY, value VARCHAR);",
        (),
    )
    .expect("Failed to create Var table");
    let f = File::open("Tupfile.ini");
    f.and_then(|&x| x.write("".as_bytes()))
        .expect("Failed to create/write to Tupfile.ini");
    println!("Finished creating tables");
}


// create a temp table from directories paths to their node ids
pub fn create_dir_pbuf_temptable(conn: &Connection) -> Result<()> {
    // https://gist.github.com/jbrown123/b65004fd4e8327748b650c77383bf553
    let stmtstr = "DROP TABLE IF EXISTS DIRPATHBUF;
CREATE TEMPORARY TABLE DIRPATHBUF AS WITH RECURSIVE fullpath (id, name, type) AS
(
    VALUES(1, '.', 2)
    UNION ALL
	SELECT  node.id, fullpath.name || '/' || node.name name, node.type
            FROM node JOIN fullpath ON node.dir=fullpath.id

) SELECT  fullpath.id, fullpath.name from fullpath where type=?";
    conn.execute(stmtstr, [crate::DirType as u8])?;
    Ok(())
}
// creates a temp table for groups
pub fn create_group_pbuf_temptable(conn: &Connection) -> Result<()> {
    let stmtstr = "
    DROP TABLE IF EXISTS GRPPATHBUF;
CREATE TEMPORARY TABLE GRPPATHBUF AS
   SELECT  node.id,DIRPATHBUF.name || '/' || node.name Name from node inner join DIRPATHBUF on
       (node.dir=DIRPATHBUF.id and node.type=?)";
    conn.execute(stmtstr, [crate::GrpType as u8])?;
    Ok(())
}

//creates a temp table for tup file paths
pub fn create_tup_pbuf_temptable(conn: &Connection) -> Result<()> {
    let stmtstr = "
DROP TABLE IF EXISTS TUPPATHBUF;
CREATE TEMPORARY TABLE TUPPATHBUF AS
SELECT node.id, node.dir, DIRPATHBUF.name || '/' || node.name name from node inner join DIRPATHBUF ON
(NODE.dir = DIRPATHBUF.id and node=?'))";
    conn.execute(stmtstr, [TupFType as u8])?;
    Ok(())
}

impl LibSqlPrepare for Connection {
    fn add_to_modify_prepare(&self) -> Result<SqlStatement> {
        let mut stmt = self.prepare("INSERT into ModifyList(id) Values (?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertMod,
        })
    }
    fn insert_dir_prepare(&self) -> Result<SqlStatement> {
        let stmt = conn.prepare("INSERT into Node (name, type) Values (?,?);")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertDir,
        })
    }
    fn insert_dir_aux_prepare(&self) -> Result<SqlStatement> {
        let stmt = conn.prepare("INSERT into PathBufDir (id, name) Values (?,?);")?;
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
        let stmt = self.prepare("INSERT into NodeLink (from_id, to_id) Values (?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertLink,
        })
    }

    fn insert_node_prepare(&self) -> Result<SqlStatement> {
        let mut stmt =
            conn.prepare("INSERT into Node (dir, name, mtime_ns, type) Values (?,?,?,?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertFile,
        })
    }
    fn find_dirid_prepare(&self) -> Result<SqlStatement> {
        let stmt = conn.prepare("SELECT id in DirPathBuf where name=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindDirId,
        })
    }

    fn fetch_node_prepare(&self) -> Result<SqlStatement> {
        let stmt = conn.prepare("SELECT id, dir, name, mtime_ns, type FROM Node where dir=? and name=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNode,
        })
    }

    fn fetch_nodes_prepare(&self) -> Result<SqlStatement> {
        let mut stmt =
            conn.prepare("SELECT id, dir, name, mtime_ns, type FROM Node where dir=?")?;
        Ok(SqlStatement(stmt, FindNodes))
    }

    fn update_mtime_prepare(&self) -> Result<SqlStatement> {
        let mut stmt = conn.prepare("UPDATE Node Set mtime_ns = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdMTime,
        })
    }

    fn update_dirid_prepare(&self) -> Result<SqlStatement> {
        let mut stmt = conn.prepare("UPDATE Node Set dir = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdDirId,
        })
    }

    fn delete_prepare(&self) -> Result<SqlStatement> {
        let mut stmt = conn.prepare("DELETE FROM Node WHERE id=?")?;
        Ok(SqlStatement(stmt, DeleteId))
    }

    fn delete_aux_prepare(&self) -> Result<SqlStatement> {
        let mut stmt = conn.prepare("INSERT into DeleteList(id) Values (?)")?;
        Ok(SqlStatement(stmt, DeleteIdAux))
    }
}
impl LibSqlExec for SqlStatement {
    fn add_to_modify_exec(&mut self, id: i64) -> Result<()> {
        anyhow::ensure!(self.tok = AddToMod, "wrong token for update to modify");
        self.stmt.insert([id])?;
        Ok(())
    }

    fn insert_dir_exec(&mut self, path_str: &str) -> Result<i64> {
        anyhow::ensure!(self.tok == InsertDir, "wrong token for Insert dir");
        self.stmt.insert([
            path_str.as_str(),
            (crate::DirType as u8).to_string().as_str(),
        ])?;
        let id = self.last_insert_rowid();
        Ok(id)
    }

    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, pbuf: P) -> Result<()> {
        anyhow::ensure!(
            self.tok == InsertDirAux,
            "wrong token for Insert Dir Into PathBufDir"
        );
        self.stmt
            .insert([id.to_string(), pbuf.as_ref().to_string_lossy().to_string()])?;
        Ok(())
    }

    fn insert_link(&mut self, from_id: i64, to_id: i64) -> Result<()> {
        anyhow::ensure!(
            self.tok = InsertLink,
            "wrong token for insert link"
        );
        self.stmt.insert([from_id, to_id])?;
        Ok(())
    }

    fn insert_sticky_link(&mut self, from_id: i64, to_id: i64) -> Result<()> {
        anyhow::ensure!(
            self.tok = InsertStickyLink,
            "wrong token for insert link"
        );
        self.stmt.insert([from_id, to_id])?;
        Ok(())
    }

    fn insert_node_exec(&mut self, n: Node, existing: &[Node]) -> Result<(i64,bool)> {
        anyhow::ensure!(self.tok == InsertFile, "wrong token for Insert file");
        for existingnode in existing {
            if existingnode.get_pid() == n.get_pid() && existingnode.get_name() == n.get_name() {
                return Ok((existingnode.id, false));
            }
        }
        self.stmt.execute([
            n.pid.to_string().as_str(),
            n.name.as_str(),
            n.mtime.to_string().as_str(),
            (n.get_type() as u8).to_string().as_str(),
        ])?;
        let id = self.last_insert_rowid();
        Ok((id, true))
    }

    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64> {
        anyhow::ensure!(self.tok == FindDirId, "wrong token for find dir");
        let q: &Path = &p;
        let id = self
            .stmt
            .query_row([q.to_string_lossy().to_string().as_str()], |r| r.get(0))?;
        Ok(id)
    }

    fn fetch_node<P: Params>(&mut self, p: P) -> Result<Node> {
        anyhow::ensure!(self.tok == FindNode, "wrong token for fetch node");
        let node = self.stmt.query_row(p, |r| make_node(r))?;
        Ok(node)
    }

    fn fetch_nodes<P: Params>(&mut self, params: P) -> Result<HashMap<i64, Node>> {
        anyhow::ensure!(self.tok == FindNodes, "wrong token for fetch node");
        let mut rows = stmt.query(params)?;
        let mut namemap = HashMap::new();
        while let Some(row) = rows.next()? {
            let node = make_node(row)?;
            namemap.insert(node.get_id(), node);
        }
        Ok(namemap)
    }

    fn update_mtime_exec(&mut self, dirid: i64, mtime_ns: i64) -> Result<()> {
        anyhow::ensure!(self.tok == UpdMTime, "wrong token for update mtime");
        self.stmt.execute([dirid, mtime_ns])?;
        Ok(())
    }

    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()> {
        anyhow::ensure!(self.tok == UpdDirId, "wrong token for dirid update");
        self.stmt.execute([dirid, id])?;
        Ok(())
    }

    fn delete_exec(&mut self, id: i64) -> Result<()> {
        anyhow::ensure(self.tok == DeleteId, "wrong token for delete node");
        self.stmt.execute([id])?;
        Ok(())
    }

    fn delete_exec_aux(&mut self, id: i64) -> Result<()> {
        anyhow::ensure(self.tok == DeleteIdAux, "wrong token for delete node");
        self.stmt.execute([id])?;
        Ok(())
    }
}

impl ForEachClauses for Connection {
    fn for_each_fnode_id<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(i64) -> Result<()>,
    {
        let mut stmt = conn.prepare(format!(
            "SELECT id from Node where type={} or type={}",
            RowType::FileType as u8,
            RowType::DirType as u8
        ));
        stmt.query_map(p, |r| -> Result<()> {
            let i = r.get(0)?;
            f(i)?;
            Ok(())
        })?;
        Ok(())
    }
    fn for_each_node_id<P, F>(&self, p: P, f: F) -> Result<()>
    where
        P: Params,
        F: FnMut(i64) -> Result<()>,
    {
        let mut stmt = conn.prepare("SELECT id from Node where type=?")?;
        stmt.query_map(p, |r| -> Result<()> {
            let i = r.get(0)?;
            f(i)?;
            Ok(())
        })?;
        Ok(())
    }
    fn for_each_tupnode_with_path<F>(&self, f: F) -> Result<()>
    where
        P: Params,
        F: FnMut(i64, &Path) -> rusqlite::Result<()>,
    {
        let mut stmt = conn.prepare("SELECT id,dir, name from TUPPATHBUF inner join ModifyList ON TupPathBuf.id = ModifyList.id")?;
        Self::for_id_and_path(f, &mut stmt)?;
        Ok(())
    }

    fn for_each_grpnode_provider<F>(&self, name: &str, f: F) -> Result<()>
    where
        F: FnMut(i64, &Path) -> rusqlite::Result<()>,
    {
        let mut stmt = conn.prepare("SELECT id from NODE where type=1 AND id in (SELECT from_id from NodeLink where to_id in
( SELECT id from GRPPATHBUF  where GRPPATHBUF.name='?'))")?;
        Self::for_id_and_path([(RuleType as u8).to_string(), name], f, &mut stmt)?;
        Ok(())
    }
    fn for_each_grpnode_requirer<F>(&self, name: &str, f: F) -> Result<()>
    where
        F: FnMut(i64, &Path) -> rusqlite::Result<()>,
    {
        let mut stmt = conn.prepare("SELECT id from NODE where type=1 AND id in (SELECT to_id from normal_link where from_id in
( SELECT id from GRPPATHBUF  where GRPPATHBUF.name='?'))")?;
        Self::for_id_and_path([(RuleType as u8).to_string(), name], f, &mut stmt)?;
        Ok(())
    }
    fn fetch_db_inputs(&self, rule_id: i64) -> Result<Vec<i64>> {
        let mut stmt = self.prepare("SELECT from_id from NodeLink where to_id = ?")?;
        Self::rule_link(rule_id, &mut stmt)
    }

    fn fetch_db_sticky_inputs(&self, rule_id: i64) -> Result<Vec<i64>> {
        let mut stmt = self.prepare("SELECT from_id from StickyLink where to_id = ?")?;
        Self::rule_link(rule_id, &mut stmt)
    }
    fn fetch_db_outputs(&self, rule_id: i64) -> Result<Vec<i64>> {
        // todo!()
        let mut stmt = self.prepare("SELECT to_id from NodeLink where from_id = ?")?;
        Self::rule_link(rule_id, &mut stmt)
    }
    fn delete_db_rule_outputs(&self, rule_id: i64) -> Result<()> {
        // todo!()
        let mut stmt = self.prepare("delete from StickyLink where from_id = ?")?;
        stmt.execute([rule_id])?;
        let mut stmt = self.prepare("delete from NodeLink where from_id = ?")?;
        stmt.execute([rule_id])?;
        Ok(())
    }
    fn delete_db_rule_inputs(&self, rule_id: i64) -> Result<()> {
        let mut stmt = self.prepare("delete from NodeLink where to_id = ?")?;
        stmt.execute([rule_id])?;
        let mut stmt = self.prepare("delete from NodeLink where to_id = ?")?;
        stmt.execute([rule_id])?;
        Ok(())
    }

    fn fetch_db_rules(&self, tup_node_dir: i64) -> Result<Vec<Node>> {
        let mut stmt = self.prepare("SELECT node.id, node.name, node.mtime_ns from NODE where node.type = ? and node.dir in\
         (SELECT from_id from NodeLink where to_id = ?)")?;
        let mut rules = Vec::new();
        let _ = stmt.query_map([RuleType, tup_node_dir], |r| -> Result<()> {
            let id = r.get(0)?;
            let rule_str: &str = r.get(1)?;
            let mtime: i64 = r.get(2)?;
            let node = Node::new(id, tup_node_dir, mtime, rule_str.to_string(), RuleType);
            rules.push(node);
            Ok(())
        })?;
        return Ok(rules);
    }

    fn for_id_and_path<P, F>(p: P, f: F, stmt: &mut Statement) -> rusqlite::Result<()>
    where
        P: Params,
        F: FnMut(i64, &Path) -> rusqlite::Result<()>,
    {
        let _ = stmt.query_map(p, |r| -> Result<()> {
            let i: i64 = r.get(0)?;
            let name = r.get(1)?;
            f(i, Path::new(name))?;
            Ok(())
        })?;
        Ok(())
    }
    fn rule_link(rule_id: i64, stmt: &mut Statement) -> Result<Vec<i64>> {
        let mut inputs = Vec::new();
        let _ = stmt.query_map([rule_id], |r| -> Result<()> {
            let id = r.get(0)?;
            inputs.push(id);
            Ok(())
        })?;
        Ok(inputs)
    }
}

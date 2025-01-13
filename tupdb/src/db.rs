use rusqlite::{Connection, Row, Transaction};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::path::{Path, MAIN_SEPARATOR};

use crate::db::RowType::{DirGen, GenF};
use crate::deletes::LibSqlDeletes;
use crate::error::{AnyError, DbResult, SqlResult};
use crate::inserts::LibSqlInserts;
use crate::queries::LibSqlQueries;

//returns change status of a db command
pub enum UpsertStatus {
    Inserted(i64),
    Updated(i64),
    Unchanged(i64),
}
#[repr(C)]
pub enum IOClass {
    File = 0,
    Dir = 1,
    Either = 2,
}
impl IOClass {
    pub fn allows_file(&self) -> bool {
        if let IOClass::File | IOClass::Either = self {
            true
        } else {
            false
        }
    }
    pub fn allows_dir(&self) -> bool {
        if let IOClass::Dir | IOClass::Either = self {
            true
        } else {
            false
        }
    }
}
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
    Group = 6,
    /// Generated Directory
    DirGen = 7,
    /// Excluded file pattern
    Excluded = 8,
    /// Glob input type for a rule
    Glob = 9,
    /// named Task that may not have any outputs or inputs
    Task = 10,
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

//const ENVDIR: i8 = -2;

impl Display for RowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (*self as u8).to_string())
    }
}

impl TryFrom<u8> for RowType {
    type Error = u8;
    fn try_from(value: u8) -> Result<Self, u8> {
        if value == 0 {
            Ok(Self::File)
        } else if value == 1 {
            Ok(Self::Rule)
        } else if value == 2 {
            Ok(Self::Dir)
        } else if value == 3 {
            Ok(Self::Env)
        } else if value == 4 {
            Ok(GenF)
        } else if value == 5 {
            Ok(Self::TupF)
        } else if value == 6 {
            Ok(Self::Group)
        } else if value == 7 {
            Ok(DirGen)
        } else if value == 8 {
            Ok(Self::Excluded)
        } else if value == 9 {
            Ok(Self::Glob)
        } else {
            Err(value)
        }
    }
}
impl RowType {
    pub fn is_dir(&self) -> bool {
        self.eq(&RowType::Dir) || self.eq(&DirGen)
    }
    pub fn is_file(&self) -> bool {
        self.eq(&RowType::File) || self.eq(&GenF) || self.eq(&RowType::TupF)
    }
    pub fn is_generated(&self) -> bool {
        self.eq(&GenF) || self.eq(&DirGen)
    }
}
/// Fields in the Node table in the database
#[derive(Clone, Debug)]
pub struct Node {
    // id of this node in the database or a descriptor that is temporary
    id: i64,
    /// parent folder id
    dirid: i64,
    /// time in nano secs
    mtime: i64,
    /// name of file or dir or rule or group
    name: String,
    /// type of data in this row
    rtype: RowType,
    /// rule display
    display_str: String,
    /// flags for rule that appear in description
    flags: String,
    /// dir of rule which created this node came about
    srcid: i64,
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
impl Default for Node {
    fn default() -> Self {
        Node::new(-1, -1, -1, String::default(), RowType::File)
    }
}
impl Node {
    pub fn root() -> Node {
        Node {
            id: 0,
            dirid: 0,
            mtime: 0,
            name: "".to_string(),
            rtype: RowType::Dir,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid: -1,
        }
    }
    pub fn new(id: i64, dirid: i64, mtime: i64, name: String, rtype: RowType) -> Node {
        Node {
            id,
            dirid,
            mtime,
            name,
            rtype,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid: -1,
        }
    }
    #[allow(dead_code)]
    pub fn unknown_with_dir(dirid: i64, name: &str, rtype: &RowType) -> Node {
        Node {
            id: -1,
            dirid,
            mtime: -1,
            name: name.to_string(),
            rtype: *rtype,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid: -1,
        }
    }
    pub fn copy_from(id: i64, n: &Node) -> Node {
        Node {
            id,
            dirid: n.dirid,
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
        dirid: i64,
        mtime: i64,
        name: String,
        rtype: RowType,
        srcid: i64,
    ) -> Node {
        Node {
            id,
            dirid,
            mtime,
            name,
            rtype,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid,
        }
    }
    pub fn new_rule(
        id: i64,
        dirid: i64,
        name: String,
        display_str: String,
        flags: String,
        srcid: u32,
    ) -> Node {
        Node {
            id,
            dirid,
            mtime: 0,
            name,
            rtype: RowType::Rule,
            display_str,
            flags,
            srcid: srcid as i64,
        }
    }
    pub fn new_task(
        id: i64,
        dirid: i64,
        name: String,
        display_str: String,
        flags: String,
        srcid: u32,
    ) -> Node {
        Node {
            id,
            dirid,
            mtime: 0,
            name,
            rtype: RowType::Task,
            display_str,
            flags,
            srcid: srcid as i64,
        }
    }
    pub fn new_grp(id: i64, dirid: i64, name: String) -> Node {
        Node {
            id,
            dirid,
            mtime: 0,
            name,
            rtype: RowType::Group,
            display_str: "".to_string(),
            flags: "".to_string(),
            srcid: -1,
        }
    }
    pub fn new_env(id: i64, dirid: i64, name: String, value: String) -> Node {
        Node {
            id,
            dirid,
            mtime: 0,
            name,
            rtype: RowType::Env,
            display_str: value,
            flags: "".to_string(),
            srcid: -1,
        }
    }
    pub fn get_id(&self) -> i64 {
        self.id
    }
    pub fn get_dir(&self) -> i64 {
        self.dirid
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
    // display str for rule is the text displayed to user while executing, for env vars it is its value, for recursive globs the directory glob pattern
    pub fn get_display_str(&self) -> &str {
        self.display_str.as_str()
    }
    /// Flags on the node indicating how to execute a rule
    pub fn get_flags(&self) -> &str {
        self.flags.as_str()
    }
    /// Source rule id (for generated file)
    pub fn get_srcid(&self) -> i64 {
        self.srcid
    }

    /// Check if the node is a generated file or directory
    pub fn is_generated(&self) -> bool {
        self.rtype == GenF || self.rtype == DirGen
    }
}

pub trait MiscStatements {
    /// Populate deletelist (DeleteList = ids of type file/dir Node  - ids in PresentList)
    /// Enrich the modified list with more entries determined by dependencies between nodes
    fn enrich_modified_list(&self) -> DbResult<()>;
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

pub fn delete_db() -> DbResult<()> {
    std::fs::remove_dir_all(".tup").map_err(|e| AnyError::from(e.to_string()))?;
    Ok(())
}
// Handle the tup init subcommand. This creates the file .tup\db and adds the tables
pub fn init_db() -> DbResult<()> {
    println!("Creating a new db.");
    //use std::fs;
    std::fs::create_dir_all(".tup").expect("Unable to access .tup dir");
    let conn = Connection::open(".tup/db").expect("Failed to connect to .tup\\db");
    if is_initialized(&conn, "Node") {
        return Err(AnyError::from(String::from("Node table already exists in .tup/db")));
    }
    conn.execute_batch(include_str!("sql/node_table.sql"))?;

    let _ = File::create("Tupfile.ini").map_err(|e| AnyError::from(e.to_string()))?;
    println!("Database created successfully.");
    Ok(())
}

pub struct TupConnection(Connection);
pub struct TupConnectionRef<'a>(&'a Connection);

pub struct TupTransaction<'a>(Transaction<'a>);

impl<'a> TupTransaction<'a> {
    fn new(t: Transaction) -> TupTransaction {
        TupTransaction(t)
    }
    pub fn commit(self) -> DbResult<()> {
        self.0.commit()?;
        Ok(())
    }
    pub fn rollback(self) -> DbResult<()> {
        self.0.rollback()?;
        Ok(())
    }

    pub fn connection(&self) -> TupConnectionRef {
        TupConnectionRef(self.0.deref())
    }



}

impl <'a> Deref for TupTransaction<'a> {
    type Target = Transaction<'a>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}



impl TupConnection {
    fn new(conn: Connection) -> Self {
        TupConnection(conn)
    }

    pub fn transaction(&mut self) -> DbResult<TupTransaction<'_>> {
        Ok(TupTransaction::new(self.0.transaction()?))
    }

    pub fn as_ref(&self) -> TupConnectionRef {
        TupConnectionRef(&self.0)
    }

}

impl Deref for TupConnection {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Deref for TupConnectionRef<'_> {
    type Target = Connection;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Log sqlite version
pub fn log_sqlite_version()
{
    log::info!("Sqlite version: {}\n", rusqlite::version());
}
impl DerefMut for TupConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
pub fn start_connection() -> DbResult<TupConnection> {
    let conn = Connection::open(".tup/db")
        .expect("Connection to tup database in .tup/db could not be established");
    if !is_initialized(&conn, "Node") {
        return Err(AnyError::from(String::from("Node table not found in .tup/db")));
    }
    Ok(TupConnection::new(conn))
}

// create a temp table from directories paths to their node ids
pub fn create_path_buf_temptable(conn: &TupConnection) -> SqlResult<()> {
    // https://gist.github.com/jbrown123/b65004fd4e8327748b650c77383bf553
    //let dir : u8 = RowType::Dir as u8;
    let s = include_str!("sql/dirpathbuf_temptable.sql");
    conn.execute_batch(s)?;
    Ok(())
}

pub fn create_dyn_io_temp_tables(conn: &TupConnection) -> SqlResult<()> {
    conn.execute_batch(include_str!("sql/dynio.sql"))?;
    Ok(())
}

//creates a temp table
pub fn create_temptables(conn: &TupConnection) -> SqlResult<()> {
    let stmt = include_str!("sql/temptables.sql");
    conn.execute_batch(stmt)?;
    Ok(())
}

pub fn db_path_str<P: AsRef<Path>>(p: P) -> String {
    let input_path_str = p.as_ref().to_string_lossy();
    static UNIX_SEP: char = '/';
    static UNIX_SEP_STR: &str = "/";
    static CD_UNIX_SEP_STR: &str = "./";
    let mut path_str = if MAIN_SEPARATOR != UNIX_SEP {
        input_path_str
            .into_owned()
            .replace(std::path::MAIN_SEPARATOR_STR, UNIX_SEP_STR)
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

impl MiscStatements for TupConnection {
    // add all directories a glob has to watch for changes

    fn enrich_modified_list(&self) -> DbResult<()> {
        // add parent dirs to modify list if children of that directory are already in it
        //let rtype = RowType::Rule as u8;
        //let grptype = RowType::Grp as u8;
        {
            // add parent dirs/glob to modify list if any child of that directory with matched glob pattern is in the delete list/modify list

            // delete entries in modify list if they appear in delete list
            // mark glob patterns as modified if any of its inputs are in the deletelist or modified list
            // note that glob patterns are mapped to Tupfiles in  NormalLink table
            // which will then trigger parsing of these tupfiles

            self.delete_tupentries_in_deleted_tupfiles()?;
            self.add_rules_with_changed_io_to_modify_list()?;

            for (i, sha) in self.fetch_modified_globs()?.into_iter() {
                self.update_node_sha_exec(i, sha.as_str())?;
                //   self.mark_modified(i, &Glob)?;
                self.mark_dependent_tupfiles_of_glob(i)?;
            }
            self.mark_dependent_tupfiles_groups()?;
            self.prune_modify_list_of_inputs_and_outputs()?;
            self.delete_nodes()?;

            //self.mark_globs_modified(compute_node_sha)?;

            // delete rules that are no longer in any tupfile

            // mark node as Modified if any of its inputs are in the deletelist or modified list
            /*let mut stmt = self.prepare(
                "Insert or IGNORE into ModifyList SELECT id, type from Node where id in \
            (SELECT to_id from NormalLink where from_id in (SELECT id from DeleteList  UNION SELECT id from ModifyList) )"
            )?;
            stmt.execute([])?; */

            // delete normal links with from / to id in deletelist
            // let mut stmt = self.prepare("DELETE from NormalLink where from_id in (SELECT id from DeleteList) or to_id in (SELECT id from DeleteList)")?;
            // stmt.execute([])?;

            // recursively mark all rules as modified if any of the parent rules are marked as modified
            // this is the closure operation.
            /* let mut stmt = self
                            .prepare(&*format!(
                                "WITH RECURSIVE NodeChain(id, type) AS (
              -- Step 1: Get the initial set of nodes (selected list of node ids) along with their types
              SELECT id, type
              FROM Node
              WHERE id IN (SELECT id from ModifyList where type = {rtype})
              UNION ALL
              -- Step 2: Recursively select connected nodes along with their types
              SELECT nl.to_id, nl.to_type
              FROM NodeChain nc
              JOIN NormalLink nl ON nc.id = nl.from_id
            )
            INSERT or IGNORE INTO ModifyList(id, type)
            SELECT DISTINCT id, type
            FROM NodeChain;"
                            ))
                            .unwrap();
                        stmt.execute([])?; */
        }

        Ok(())
    }
}

pub(crate) fn make_tup_node(row: &Row) -> rusqlite::Result<Node> {
    let i = row.get(0)?;
    let dir: i64 = row.get(1)?;
    let name: String = row.get(2)?;
    Ok(Node::new(i, dir, 0, name, RowType::TupF))
}

pub(crate) fn make_node(row: &Row) -> rusqlite::Result<Node> {
    let id: i64 = row.get(0)?;
    let dirid: i64 = row.get(1)?;
    let rtype: u8 = row.get(2)?;
    let name: String = row.get(3)?;
    let mtime: i64 = row.get(4)?;
    let rtype = RowType::try_from(rtype)
        .unwrap_or_else(|_| panic!("Invalid row type {} for node {}", rtype, name));
    Ok(Node::new(id, dirid, mtime, name, rtype))
}

pub(crate) fn make_rule_node(row: &Row) -> rusqlite::Result<Node> {
    let id: i64 = row.get(0)?;
    let dirid: i64 = row.get(1)?;
    let name: String = row.get(2)?;
    let display_str: String = row.get(3)?;
    let flags: String = row.get(4)?;
    let srcid: i64 = row.get(5)?;

    Ok(Node::new_rule(
        id,
        dirid,
        name,
        display_str,
        flags,
        srcid as u32,
    ))
}

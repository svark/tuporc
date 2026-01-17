use crate::db::RowType::{DirGen, GenF};
use crate::deletes::LibSqlDeletes;
use crate::error::{AnyError, DbResult, WrapError};
use crate::inserts::LibSqlInserts;
use crate::queries::LibSqlQueries;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::TransactionBehavior::Deferred;
use rusqlite::{Connection, Row, Transaction};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::path::{Path, MAIN_SEPARATOR};
use std::sync::Arc;
use std::time::Duration;
pub const MAX_CONNECTIONS : u32 = 20;
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
    pub fn new_withsrc(
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

    pub fn unknown() -> Node {
        Node {
            id: -1,
            dirid: -1,
            mtime: -1,
            name: "".to_string(),
            rtype: RowType::File,
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
    pub fn is_valid(&self) -> bool {
        self.id > 0
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
    pub fn is_file(&self) -> bool {
        self.rtype == RowType::File || self.rtype == GenF || self.rtype == RowType::TupF
    }
}

pub trait MiscStatements {
    /// Populate deletelist (DeleteList = ids of type file/dir Node  - ids in PresentList)
    /// Enrich the modified list with more entries determined by dependencies between nodes
    fn enrich_modify_list(&self) -> DbResult<()>;

    /// Set run status for a phase ("parse"/"build"/"update")
    fn set_run_status(&self, phase: &str, status: &str, ts: i64) -> DbResult<()>;
    /// Fetch current run status
    fn get_run_status(&self) -> DbResult<Option<(String, String, i64)>>;

    // mark missing not deleted FILE_SYS with checks
    fn mark_missing_not_deleted_checked(&self) -> DbResult<()>;
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

impl<'a> MiscStatements for TupTransaction<'a> {
    fn enrich_modify_list(&self) -> DbResult<()> {
        self.connection().enrich_modify_list()
    }

    fn set_run_status(&self, phase: &str, status: &str, ts: i64) -> DbResult<()> {
        self.connection().set_run_status(phase, status, ts)
    }

    fn get_run_status(&self) -> DbResult<Option<(String, String, i64)>> {
        self.connection().get_run_status()
    }

    fn mark_missing_not_deleted_checked(&self) -> DbResult<()> {
       self.connection().mark_missing_not_deleted_checked()
    }
}

impl<'a> MiscStatements for TupConnectionRef<'a> {
    fn enrich_modify_list(&self) -> DbResult<()> {
        self.mark_orphans_to_delete().map_err(AnyError::from).wrap_error("Mark orphans as deleted")?;
        self.mark_rules_with_changed_io().wrap_error("Mark rules with changed io")?;

        self.mark_tupfile_deps().wrap_error("Mark tupfile dependencies (included tupfiles->tupfiles)")?;
        for (i, sha) in self.fetch_modified_globs()?.into_iter() {
            self.update_node_sha(i, sha.as_str())?;
            self.mark_glob_deps(i)?;
        }

        self.mark_group_deps().wrap_error("Mark group dependencies")?;
        self.prune_modify_list().map_err(AnyError::from).wrap_error("Keep only rules as modified")?;
        self.delete_nodes().map_err(AnyError::from).wrap_error("Delete nodes")?;

        Ok(())
    }

    fn set_run_status(&self, phase: &str, status: &str, ts: i64) -> DbResult<()> {
        self.execute(
            "INSERT OR REPLACE INTO RunStatus (id, phase, status, ts) VALUES (1, ?1, ?2, ?3)",
            rusqlite::params![phase, status, ts],
        )?;
        Ok(())
    }

    fn get_run_status(&self) -> DbResult<Option<(String, String, i64)>> {
        let mut stmt =
            self.prepare("SELECT phase, status, ts FROM RunStatus WHERE id = 1 LIMIT 1")?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            let phase: String = row.get(0)?;
            let status: String = row.get(1)?;
            let ts: i64 = row.get(2)?;
            Ok(Some((phase, status, ts)))
        } else {
            Ok(None)
        }
    }

    fn mark_missing_not_deleted_checked(&self) -> DbResult<()> {
        self.mark_missing_not_deleted()?;
        Ok(())
    }
}

pub fn delete_db() -> DbResult<()> {
    std::fs::remove_dir_all(".tup").map_err(|e| {
        AnyError::from(format!(
            "Failed to remove existing .tup directory during reinit: {e}"
        ))
    })?;
    Ok(())
}
// Handle the tup init subcommand. This creates the file .tup\db and adds the tables
pub fn init_db() -> DbResult<()> {
    println!("Creating a new db.");
    //use std::fs;
    std::fs::create_dir_all(".tup").map_err(|e| {
        AnyError::from(format!(
            "Unable to access or create .tup directory before initializing DB: {e}"
        ))
    })?;
    let conn = Connection::open(".tup/db").map_err(|e| {
        AnyError::from(format!(
            "Failed to open connection to .tup/db while initializing: {e}"
        ))
    })?;
    if is_initialized(&conn, "Node") {
        return Err(AnyError::from(String::from(
            "Node table already exists in .tup/db",
        )));
    }
    conn.execute_batch(include_str!("sql/node_table.sql"))
        .map_err(|e| {
            AnyError::from(format!(
                "Failed to execute schema creation statements for .tup/db: {e}"
            ))
        })?;

    File::create("Tupfile.ini").map_err(|e| {
        AnyError::from(format!(
            "Failed to create Tupfile.ini after initializing database: {e}"
        ))
    })?;
    println!("Database created successfully.");
    Ok(())
}

type R2D2Pool = Arc<r2d2::Pool<SqliteConnectionManager>>;

#[derive(Clone, Debug)]
pub struct TupConnectionPool {
    pool: R2D2Pool,
}

pub struct TupConnection(r2d2::PooledConnection<SqliteConnectionManager>);

impl TupConnectionPool {
    pub fn new(database_url: &str, pool_size: u32) -> Self {
        let manager = SqliteConnectionManager::file(database_url);
        let pool = r2d2::Builder::new()
            .max_size(std::cmp::min(MAX_CONNECTIONS, pool_size))
            .connection_timeout(Duration::from_secs(10)) // Extend the timeout
            .build(manager)
            .expect("Failed to create connection pool");

        TupConnectionPool {
            pool: Arc::new(pool),
        }
    }

    pub fn get(&self) -> Result<TupConnection, r2d2::Error> {
        Ok(TupConnection::new(self.pool.get()?))
    }

    pub fn max_size(&self) -> u32 {
        self.pool.max_size()
    }
}
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

    pub fn connection(&self) -> TupConnectionRef<'_> {
        TupConnectionRef(self.0.deref())
    }
}

impl<'a> Deref for TupTransaction<'a> {
    type Target = Transaction<'a>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TupConnection {
    fn new(conn: r2d2::PooledConnection<SqliteConnectionManager>) -> Self {
        TupConnection(conn)
    }

    pub fn transaction(&mut self) -> DbResult<TupTransaction<'_>> {
        let t = self.0.unchecked_transaction()?;
        // Disable FK checks within scan/parse transactions for speed; callers should ensure consistency.
        t.execute("PRAGMA foreign_keys=OFF", [])?;
        t.execute("PRAGMA defer_foreign_keys=ON", [])?;
        Ok(TupTransaction::new(t))
    }
    pub fn deferred_transaction(&mut self) -> DbResult<TupTransaction<'_>> {
        Ok(TupTransaction::new(
            self.0.transaction_with_behavior(Deferred)?,
        ))
    }

    pub fn as_ref(&self) -> TupConnectionRef<'_> {
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

impl TupConnectionRef<'_> {
    pub fn transaction(&self) -> DbResult<TupTransaction<'_>> {
        unreachable!("Cannot start a transaction on a read-only connection")
    }
}

/// Log sqlite version
pub fn log_sqlite_version() {
    log::info!("Sqlite version: {}\n", rusqlite::version());
}
impl DerefMut for TupConnection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
pub fn start_connection(database_url: &str, max_pool_size: u32) -> DbResult<TupConnectionPool> {
    let pool = TupConnectionPool::new(database_url, max_pool_size);
    let conn = pool.get().map_err(|e| {
        AnyError::from(format!(
            "Failed to obtain connection for {database_url}: {e}"
        ))
    })?;
    if !is_initialized(&conn, "Node") {
        return Err(AnyError::from(String::from(format!(
            "Node table not found in {database_url}"
        ))));
    }
    conn.execute("PRAGMA optimize;", []).map_err(|e| {
        AnyError::from(format!(
            "Failed to run PRAGMA optimize on {database_url}: {e}"
        ))
    })?;
    #[cfg(debug_assertions)]
    {
        let mut stmt = conn.prepare("PRAGMA compile_options")?;
        let s = stmt
            .query_map([], |r| {
                // println!("Sqlite compile options: {}",
                Ok(r.get::<_, String>(0)?)
            })
            .map_err(|e| AnyError::from(e))?;
        for i in s {
            println!("Sqlite compile options: {}", i.unwrap_or("_".to_string()));
        }
    }
    Ok(pool)
}

pub fn create_dirpathbuf_temptable(conn: &mut Connection) -> DbResult<()> {
    #[cfg(debug_assertions)]
    let start_time = std::time::Instant::now();

    let dirpathbuf_create_sql = include_str!("sql/dirpathbuf_temptable.sql");
    let dirpathbuf_indices_sql = include_str!("sql/dirpathbuf_indices.sql");

    // Define the DirPath structure
    struct DirPath {
        id: i64,
        dir: i64,
        name: String,
    }

    // Initialize the stack with an estimated capacity
    let mut stack = Vec::with_capacity(1024);
    stack.push(DirPath {
        id: 1,
        dir: 0,
        name: ".".to_string(),
    });

    let tx = conn.transaction()?;

    {
        // Execute the initial setup SQL
        tx.execute_batch(dirpathbuf_create_sql)?;
        // Process directories using a stack (Depth-First Search)
        while let Some(dir_path) = stack.pop() {
            // Insert the current directory path
            tx.insert_into_dirpathuf(dir_path.id, dir_path.dir, dir_path.name.as_str())?;

            // Query child directories
            tx.for_each_subdirectory(dir_path.id, |id, name| {
                let new_name = format!("{}/{}", dir_path.name, name);
                stack.push(DirPath {
                    id,
                    dir: dir_path.id,
                    name: new_name,
                });
                Ok(())
            })?;
        }

        // Create indexes to optimize future queries
        tx.execute_batch(dirpathbuf_indices_sql)?;
    }
    tx.commit()?;

    // Log the elapsed time
    #[cfg(debug_assertions)]
    eprintln!(
        "DirPathBuf table created in {:.4} seconds.",
        start_time.elapsed().as_secs_f64(),
    );

    Ok(())
}

pub fn create_presentlist_temptable(conn: &TupConnection) -> DbResult<()> {
    let s = include_str!("sql/presentlisttemptable.sql");
    conn.execute_batch(s)?;
    Ok(())
}

pub fn create_tuppathbuf_temptable(conn: &TupConnection) -> DbResult<()> {
    let s = include_str!("sql/tuppathbuf.sql");
    conn.execute_batch(s)?;
    Ok(())
}

pub fn create_dyn_io_temp_tables(conn: &TupConnection) -> DbResult<()> {
    conn.execute_batch(include_str!("sql/dynio.sql"))?;
    Ok(())
}

//creates a temp table
pub fn create_temptables(conn: &TupConnection) -> DbResult<()> {
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
    fn enrich_modify_list(&self) -> DbResult<()> {
        TupConnectionRef(self.deref()).enrich_modify_list()?;
        Ok(())
    }

    fn set_run_status(&self, phase: &str, status: &str, ts: i64) -> DbResult<()> {
        TupConnectionRef(self.deref()).set_run_status(phase, status, ts)?;
        Ok(())
    }

    fn get_run_status(&self) -> DbResult<Option<(String, String, i64)>> {
        TupConnectionRef(self.deref()).get_run_status()
    }

    fn mark_missing_not_deleted_checked(&self) -> DbResult<()> {
        // If PresentList is empty, treating everything as missing would mark the whole tree
        // for deletion. Skip in that case and let the next scan rebuild.
       TupConnectionRef(self.deref()).mark_missing_not_deleted_checked()
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
pub(crate) fn make_task_node(row: &Row) -> rusqlite::Result<Node> {
    let id: i64 = row.get(0)?;
    let dirid: i64 = row.get(1)?;
    let name: String = row.get(2)?;
    let display_str: String = row.get(3)?;
    let flags: String = row.get(4)?;
    let srcid: i64 = row.get(5)?;

    Ok(Node::new_task(
        id,
        dirid,
        name,
        display_str,
        flags,
        srcid as u32,
    ))
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

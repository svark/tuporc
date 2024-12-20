use std::cmp::Ordering;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::path::{Path, PathBuf, MAIN_SEPARATOR};

use log::debug;
use pathdiff::diff_paths;
use rusqlite::{Connection, Params, Row, Statement};
use tupparser::paths::MatchingPath;

use crate::db::RowType::GenF;
use crate::db::StatementType::*;

#[derive(Debug)]
pub struct CallBackError {
    inner: String,
}

impl CallBackError {
    pub fn from(inner: String) -> Self {
        Self { inner }
    }
}

#[derive(Debug)]
pub enum AnyError {
    Db(rusqlite::Error),
    CbErr(CallBackError),
}

impl Display for AnyError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AnyError::Db(e) => e.fmt(f),
            AnyError::CbErr(e) => e.inner.fmt(f),
        }
    }
}

impl Error for AnyError {}

impl From<CallBackError> for AnyError {
    fn from(value: CallBackError) -> Self {
        AnyError::CbErr(value)
    }
}

impl From<String> for AnyError {
    fn from(value: String) -> Self {
        AnyError::CbErr(CallBackError::from(value))
    }
}

impl From<rusqlite::Error> for AnyError {
    fn from(value: rusqlite::Error) -> Self {
        AnyError::Db(value)
    }
}

impl AnyError {
    pub(crate) fn has_no_rows(&self) -> bool {
        match self {
            AnyError::Db(e) => e.eq(&rusqlite::Error::QueryReturnedNoRows),
            _ => false,
        }
    }
}

pub(crate) type Result<T> = std::result::Result<T, AnyError>;

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

const ENVDIR: i8 = -2;

impl Display for RowType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", (*self as u8).to_string())
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
        } else if value == 9 {
            Ok(Self::Glob)
        } else {
            Err(value)
        }
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

impl Node {}

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
            rtype: RowType::Grp,
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
pub(crate) enum StatementType {
    #[allow(dead_code)]
    DoNothing,
    AddToMod,
    AddToDel,
    AddToPres,
    #[allow(dead_code)]
    AddToTempIds,
    InsertDir,
    InsertDirAux,
    RemoveDirAux,
    InsertTupAux,
    RemoveTupAux,
    InsertFile,
    InsertEnv,
    InsertLink,
    //InsertFnTrace,
    FindDirId,
    FindDirIdWithPar,
    #[allow(dead_code)]
    FindGroupId,
    FindNode,
    FindNodeId,
    FindNodeById,
    FindEnvId,
    FindRuleNodes,
    FindGlobNodes,
    FindNodePath,
    #[allow(dead_code)]
    FindNodeByPath,
    FindParentRule,
    //FindIo,
    UpdateMTime,
    UpdateType,
    UpdDisplayStr,
    UpdateFlags,
    UpdateSrcId,
    #[allow(dead_code)]
    UpdateDirId,
    #[allow(dead_code)]
    UpdateEnv,
    DeleteId,
    DeleteNormalRuleLinks,
    ImmediateDeps,
    RuleSuccess,
    #[allow(dead_code)]
    RuleDepRules,
    RestoreDeleted,
    #[allow(dead_code)]
    FetchIO,
    #[allow(dead_code)]
    FetchEnvsForRule,
    #[allow(dead_code)]
    RemovePresents,
    #[allow(dead_code)]
    RemoveSuccess,
    #[allow(dead_code)]
    PruneMod,
    FetchOutputsForRule,
    FetchInputsForRule,
    FetchFlagsForRule,
    InsertTrace,
    FetchMonitored,
    InsertMonitored,
    #[allow(dead_code)]
    RemoveMonitoredByPath,
    #[allow(dead_code)]
    RemoveMonitoredById,
    FetchMessage,
    #[allow(dead_code)]
    FindFirstParentContaining,
}

pub struct SqlStatement<'conn> {
    stmt: Statement<'conn>,
    tok: StatementType,
}

pub(crate) trait LibSqlPrepare {
    fn add_to_modify_prepare(&self) -> Result<SqlStatement>;
    fn add_to_delete_prepare(&self) -> Result<SqlStatement>;
    fn add_to_present_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn remove_success_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn prune_modified_of_success_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn insert_dir_prepare(&self) -> Result<SqlStatement>;
    fn insert_dir_aux_prepare(&self) -> Result<SqlStatement>;
    fn remove_dir_aux_prepare(&self) -> Result<SqlStatement>;
    fn insert_tup_aux_prepare(&self) -> Result<SqlStatement>;
    fn remove_tup_aux_prepare(&self) -> Result<SqlStatement>;
    fn insert_link_prepare(&self) -> Result<SqlStatement>;
    fn insert_node_prepare(&self) -> Result<SqlStatement>;
    fn insert_env_prepare(&self) -> Result<SqlStatement>;
    fn fetch_dirid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_dirid_with_par_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn fetch_groupid_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn fetch_node_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn fetch_node_insensitive_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn do_nothing_prepare(&self) -> Result<SqlStatement>;
    fn fetch_nodeid_prepare(&self) -> Result<SqlStatement>;
    fn fetch_node_by_id_prepare(&self) -> Result<SqlStatement>;
    fn fetch_env_id_prepare(&self) -> Result<SqlStatement>;
    fn fetch_rules_nodes_prepare_by_dirid(&self) -> Result<SqlStatement>;
    fn fetch_glob_nodes_prepare(&self, is_recursive: bool) -> Result<SqlStatement>;
    fn fetch_node_path_prepare(&self) -> Result<SqlStatement>;
    fn fetch_parent_rule_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn find_node_by_path_prepare(&self) -> Result<SqlStatement>;
    fn update_mtime_prepare(&self) -> Result<SqlStatement>;
    fn update_type_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn update_dirid_prepare(&self) -> Result<SqlStatement>;
    fn update_display_str_prepare(&self) -> Result<SqlStatement>;
    fn update_flags_prepare(&self) -> Result<SqlStatement>;
    fn update_srcid_prepare(&self) -> Result<SqlStatement>;
    fn update_env_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn delete_prepare(&self) -> Result<SqlStatement>;
    fn delete_tup_rule_links_prepare(&self) -> Result<SqlStatement>;
    fn mark_outputs_deleted_prepare(&self) -> Result<SqlStatement>;
    fn mark_rule_success_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn get_rule_deps_tupfiles_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn restore_deleted_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn mark_deleted_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn fetch_io_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn fetch_envs_for_rule_prepare(&self) -> Result<SqlStatement>;
    fn fetch_outputs_for_rule_prepare(&self) -> Result<SqlStatement>;
    fn fetch_inputs_for_rule_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn fetch_flags_for_rule_prepare(&self) -> Result<SqlStatement>;
    fn insert_trace_prepare(&self) -> Result<SqlStatement>;
    fn insert_monitored_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn remove_monitored_by_id_prepare(&self) -> Result<SqlStatement>;
    #[allow(dead_code)]
    fn remove_monitored_gen_files_prepare(&self) -> Result<SqlStatement>;
    fn fetch_monitored_prepare(&self) -> Result<SqlStatement>;
    // fetch the next message from build system.
    #[allow(dead_code)]
    fn fetch_message_prepare(&self) -> Result<SqlStatement>;
}

pub(crate) trait LibSqlExec {
    /// Add a node to the modify list. These are nodes(files rules, etc.) that have been modified since the last run
    fn add_to_modify_exec(&mut self, node_id: i64, rtyp: RowType) -> Result<()>;
    /// Add a node to the delete list. These are nodes(files rules, etc.) that have been deleted since the last run
    fn add_to_delete_exec(&mut self, node_id: i64, rtype: RowType) -> Result<()>;
    /// Add a node to the present list. These are nodes(files rules, etc.) that still exists after the last run
    fn add_to_present_exec(&mut self, node_id: i64, rtype: RowType) -> Result<()>;
    /// Insert a directory node into the database
    #[allow(dead_code)]
    fn insert_dir_exec(&mut self, path_str: &str, dir: i64) -> Result<i64>;
    /// Insert a directory node into the database. This version is used to keep track of full paths of the dirs in an auxiliary table
    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()>;
    fn insert_tup_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()>;
    fn remove_tup_aux_exec(&mut self, id: i64) -> Result<()>;
    fn remove_dir_aux_exec(&mut self, id: i64) -> Result<()>;

    /// Insert a link between two nodes into the database. This is used to keep track of the dependencies between nodes and build a graph
    fn insert_link(
        &mut self,
        from_id: i64,
        to_id: i64,
        is_sticky: bool,
        to_type: RowType,
    ) -> Result<()>;
    /// Insert a sticky link between two nodes into the database. This is used to keep track of the dependencies between nodes and build a graph
    //fn insert_sticky_link(&mut self, from_id: i64, to_id: i64) -> Result<()>;
    /// insert a node into the database
    fn insert_node_exec(&mut self, n: &Node) -> Result<i64>;
    /// Insert an env key value pair into the database
    fn insert_env_exec(&mut self, env: &str, val: &str) -> Result<i64>;
    /// fetch directory node id from its path.
    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64>;
    /// fetch directory node id from its path and its parent directory id.
    fn fetch_dirid_with_par<P: AsRef<Path>>(&mut self, p: P) -> Result<(i64, i64)>;
    /// fetch group id from its name and directory
    #[allow(dead_code)]
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
    fn fetch_glob_nodes<F, P>(
        &mut self,
        base_path: P,
        gname: P,
        recursive: bool,
        f: F,
    ) -> Result<Vec<MatchingPath>>
    where
        F: FnMut(&String) -> Option<MatchingPath>,
        P: AsRef<Path>;
    /// fetch rule that produces the given node
    fn fetch_parent_rule(&mut self, id: i64) -> Result<Vec<i64>>;
    /// fetch node path from its name and directory id
    #[allow(dead_code)]
    fn fetch_node_path(&mut self, name: &str, dirid: i64) -> Result<PathBuf>;
    fn fetch_node_dir_path(&mut self, dirid: i64) -> Result<PathBuf>;
    /// update modified time for a node with the given id
    fn update_mtime_exec(&mut self, id: i64, mtime_ns: i64) -> Result<()>;
    fn update_type_exec(&mut self, id: i64, row_type: RowType) -> Result<()>;
    fn update_display_str(&mut self, id: i64, display_str: &str) -> Result<()>;
    fn update_flags_exec(&mut self, id: i64, flags: &str) -> Result<()>;
    fn update_srcid_exec(&mut self, id: i64, srcid: i64) -> Result<()>;
    /// update directory id for a node with the given id
    #[allow(dead_code)]
    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()>;
    /// update env value  for an env node with the given id
    fn update_env_exec(&mut self, id: i64, val: &str) -> Result<()>;
    /// delete a node with the given id
    #[allow(dead_code)]
    fn delete_exec(&mut self, id: i64) -> Result<()>;
    /// deleted nodes that are outputs of a rule with the given rule id
    fn mark_rule_outputs_deleted(&mut self, rule_id: i64) -> Result<()>;
    fn mark_rule_succeeded(&mut self, rule_id: i64) -> Result<()>;
    /// fetch all tupfiles that depend on the given rule (usually via a group output)
    #[allow(dead_code)]
    fn fetch_dependent_tupfiles(&mut self, rule_id: i64) -> Result<Vec<Node>>;
    /// delete normal links between nodes that are inputs and outputs of a rule with the given rule id
    fn delete_normal_rule_links(&mut self, rule_id: i64) -> Result<()>;
    /// find node by path
    #[allow(dead_code)]
    fn find_node_by_path<P: AsRef<Path>>(&mut self, dir_path: P, name: &str) -> Result<(i64, i64)>;
    /// fetch all read write operations performed by a process with given pid
    fn fetch_io(&mut self, proc_id: i32) -> Result<Vec<(String, u8)>>;
    /// fetch all envs necessary for the given rule
    #[allow(dead_code)]
    fn fetch_envs(&mut self, rule_id: i32) -> Result<Vec<String>>;
    /// fetch all inputs of the given rule
    fn fetch_inputs(&mut self, rule_id: i32) -> Result<Vec<Node>>;
    /// fetch flags of the given rule
    fn fetch_flags(&mut self, rule_id: i32) -> Result<Option<String>>;
    /// fetch all outputs of the given rule
    fn fetch_outputs(&mut self, rule_id: i32) -> Result<Vec<Node>>;
    fn insert_trace<P: AsRef<Path>>(
        &mut self,
        path: P,
        pid: i32,
        gen: u32,
        typ: u8,
        childcnt: i32,
    ) -> Result<()>;

    fn insert_monitored<P: AsRef<Path>>(&mut self, p: P, keyword_id: i64, event: i32)
        -> Result<()>;
    fn fetch_monitored(&mut self, generation_id: i64) -> Result<Vec<(String, bool)>>;
    #[allow(dead_code)]
    fn remove_monitored_by_path<P: AsRef<Path>>(&mut self, generation_id: i64) -> Result<()>;
    #[allow(dead_code)]
    fn remove_monitored_by_generation_id(&mut self, generation_id: i64) -> Result<()>;
}
pub(crate) trait MiscStatements {
    /// Populate deletelist (DeleteList = ids of type file/dir Node  - ids in PresentList)
    fn populate_delete_list(&self) -> Result<()>;
    fn first_containing(&self, name: &str, dir: i64) -> Result<(String, i64)>;
    /// Enrich the modified list with nodes that have been modified outside of tup
    fn enrich_modified_list_with_outside_mods(&self) -> Result<()>;
    /// Enrich the modified list with more entries determined by dependencies between nodes
    fn enrich_modified_list(&self) -> Result<()>;
    ///Remove given id from the delete list
    fn remove_id_from_delete_list(&self, id: i64) -> Result<()>;
    fn prune_present_list(&self) -> Result<()>;
    /// Remove entries in modify list that are in successlist(rules that exectued successfuly) or deletelist
    fn prune_modified_list(&self) -> Result<()>;

    /// Delete nodes marked in DeleteList
    fn delete_nodes(&self) -> Result<()>;
    #[allow(dead_code)]
    fn remove_modified_list(&self) -> Result<()>;
    #[allow(dead_code)]
    fn write_message(&self, message: &str) -> Result<()>;
    #[allow(dead_code)]
    fn read_message(&self, last_id: i64) -> Result<String>;
}
pub(crate) trait ForEachClauses {
    #[allow(dead_code)]
    fn for_each_file_node_id<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(i64) -> Result<()>;
    #[allow(dead_code)]
    fn for_each_node_id<P, F>(&self, p: P, f: F) -> Result<()>
    where
        P: Params,
        F: FnMut(i64) -> Result<()>;
    fn for_changed_or_created_tup_node_with_path<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>;
    // all the tupfiles in the db
    fn for_tup_node_with_path<F>(&self, f: F) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>;
    #[allow(dead_code)]
    fn for_each_grp_nodeid_provider<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(i64) -> Result<()>;
    #[allow(dead_code)]
    fn for_each_grp_node_provider<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>;
    #[allow(dead_code)]
    fn for_each_grp_node_requirer<F>(
        &self,
        group_id: i64,
        rtype: Option<RowType>,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(Node) -> Result<()>;
    #[allow(dead_code)]
    fn fetch_db_sticky_inputs(&self, rule_id: i64) -> Result<Vec<i64>>;
    #[allow(dead_code)]
    fn fetch_db_inputs(&self, rule_id: i64) -> Result<Vec<i64>>;
    #[allow(dead_code)]
    fn fetch_db_outputs(&self, rule_id: i64) -> Result<Vec<i64>>;

    fn for_tupid_and_path<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
    where
        P: Params,
        F: FnMut(Node) -> Result<()>;
    #[allow(dead_code)]
    fn for_id<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
    where
        P: Params,
        F: FnMut(i64) -> Result<()>;
    fn for_node<P, F>(p: P, f: F, stmt: &mut Statement) -> Result<()>
    where
        P: Params,
        F: FnMut(Node) -> Result<()>;
    #[allow(dead_code)]
    fn rule_link(rule_id: i64, stmt: &mut Statement) -> Result<Vec<i64>>;
    fn for_each_link<F>(&mut self, f: F) -> Result<()>
    where
        F: FnMut(i32, i32) -> Result<()>;
    //fn populate_delete_list(&self) -> Result<()>;
    fn rules_to_run_no_target<F>(&self, f: F) -> Result<Vec<Node>>
    where
        F: FnMut(&Node) -> Result<Node>;
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
    conn.execute_batch(include_str!("sql/node_table.sql"))
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

    fn remove_success_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE * from SuccessList")?;
        Ok(SqlStatement {
            stmt,
            tok: RemoveSuccess,
        })
    }

    // remove rules that succeeded and their inputs from the modify list
    fn prune_modified_of_success_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(
            "DELETE from ModifyList where id in (SELECT id from SuccessList \
             UNION SELECT from_id from NormalLink where to_id in (SELECT id from SuccessList))",
        )?;
        Ok(SqlStatement {
            stmt,
            tok: PruneMod,
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
    fn remove_dir_aux_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE from DirPathBuf where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: RemoveDirAux,
        })
    }

    fn insert_tup_aux_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("INSERT into TupPathBuf (id, name) Values (?,?);")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertTupAux,
        })
    }
    fn remove_tup_aux_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE from TupPathBuf where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: RemoveTupAux,
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
        let stmt = self.prepare(&*format!(
            "INSERT into Node (dir, name, type, display_str) Values ({e}, ?, {rtype}, ?)"
        ))?;
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
    fn fetch_dirid_with_par_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT id, dir FROM DirPathBuf where name=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindDirIdWithPar,
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
    fn fetch_node_insensitive_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(
            "SELECT id, dir, type, name, mtime_ns FROM Node where dir=? and LOWER(name)=?",
        )?;
        Ok(SqlStatement {
            stmt,
            tok: FindNode,
        })
    }

    fn do_nothing_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT 1")?;
        Ok(SqlStatement {
            stmt,
            tok: DoNothing,
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
        let stmt = self.prepare("SELECT id, dir, type, name, mtime_ns  FROM Node where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodeById,
        })
    }

    fn fetch_env_id_prepare(&self) -> Result<SqlStatement> {
        let e = ENVDIR;
        let stmt = self.prepare(&*format!(
            "Select id, display_str from node where name=? and dir={e}"
        ))?;
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
            tok: FindRuleNodes,
        })
    }

    fn fetch_glob_nodes_prepare(&self, is_recursive: bool) -> Result<SqlStatement> {
        let ftype = RowType::File as u8;
        let dirtype = RowType::Dir as u8;
        let gen_ftype = RowType::GenF as u8;
        let op = if is_recursive { "glob" } else { "=" };
        let statement = format!(
            "SELECT name from Node where name glob ? and dir in (SELECT id FROM DirPathBuf where name {op} ?) \
         and (type = {ftype} or type = {dirtype} or type = {gen_ftype})"
        );
        let stmt = self.prepare(statement.as_str())?;
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
        let stmtstr = "SELECT Node.id, Node.dir from Node where Node.name = ? and dir in \
        (SELECT id from DIRPATHBUF where DIRPATHBUF.name=?)";
        let stmt = self.prepare(stmtstr)?;
        Ok(SqlStatement {
            stmt,
            tok: FindNodeByPath,
        })
    }

    fn update_mtime_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set mtime_ns = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdateMTime,
        })
    }
    fn update_type_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set type = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdateType,
        })
    }

    fn update_dirid_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set dir = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdateDirId,
        })
    }
    fn update_display_str_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set display_str = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdDisplayStr,
        })
    }
    fn update_flags_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set flags = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdateFlags,
        })
    }
    fn update_srcid_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("UPDATE Node Set srcid = ? where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdateSrcId,
        })
    }
    fn update_env_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("Update Node Set display_str = ? where id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: UpdateEnv,
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
        Ok(SqlStatement {
            stmt,
            tok: DeleteNormalRuleLinks,
        })
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

    fn mark_rule_success_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(" INSERT or IGNORE into SuccessList (id) SELECT ? UNION ALL SELECT from_id from NormalLink where to_id=?")?;
        Ok(SqlStatement {
            stmt,
            tok: RuleSuccess,
        })
    }

    fn get_rule_deps_tupfiles_prepare(&self) -> Result<SqlStatement> {
        let rtype = RowType::Rule as u8;
        let stmt = self.prepare(&*format!(
            "SELECT id, dir, name from TUPPATHBUF where dir in
             (SELECT dir from Node where type = {rtype} and id in
            (WITH RECURSIVE dependents(x) AS (
   SELECT ?
   UNION
  SELECT to_id FROM NormalLink JOIN dependents ON NormalLink.from_id=x
)
SELECT DISTINCT x FROM dependents));",
        ))?;

        // placeholder ? here is the rule_id which initiates the search (base case in recursive query)
        Ok(SqlStatement {
            stmt,
            tok: RuleDepRules,
        })
    }

    fn restore_deleted_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE from DeletedList where id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: RestoreDeleted,
        })
    }

    fn mark_deleted_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("Insert into DeletedList (id) VALUES(?)")?;
        Ok(SqlStatement {
            stmt,
            tok: RestoreDeleted,
        })
    }
    // get the files written or read by a process (pid) in the latest generation. Latest gen is the one with the highest generation value
    // Latest gen filtering happens after the query results are returned
    fn fetch_io_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT path, typ, gen from DYNIO where pid = ?")?;
        Ok(SqlStatement { stmt, tok: FetchIO })
    }

    // Envs that this rule depends. The `Export` keyword in Tupfile makes the envs available to the rule
    fn fetch_envs_for_rule_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(
            "SELECT name from ENV where id in (SELECT from_id from NormalLink where to_id = ?)",
        )?;
        Ok(SqlStatement {
            stmt,
            tok: FetchEnvsForRule,
        })
    }

    fn fetch_outputs_for_rule_prepare(&self) -> Result<SqlStatement> {
        let typ = RowType::GenF as u8;
        let stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, Node.type type,\
         (DirPathBuf.name || '/' || Node.name) name, Node.mtime_ns mtime_ns from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id)\
         where  Node.id in \
        (SELECT to_id from NormalLink where from_id  = ? and to_type={typ})"))?;
        Ok(SqlStatement {
            stmt,
            tok: FetchOutputsForRule,
        })
    }

    fn fetch_inputs_for_rule_prepare(&self) -> Result<SqlStatement> {
        let file_type = RowType::File as u8;
        let gen_file_type = RowType::GenF as u8;
        let stmt = self.prepare(&format!("SELECT Node.id id, Node.dir dir, Node.type type, \
        (DirPathBuf.name || '/' || Node.name) name, Node.mtime_ns mtime_ns from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id) \
          where (Node.type = {file_type} or Node.type = {gen_file_type}) and Node.id in \
        (SELECT from_id from NormalLink where to_id  = ?)"))?;
        Ok(SqlStatement {
            stmt,
            tok: FetchInputsForRule,
        })
    }
    fn fetch_flags_for_rule_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare(&"SELECT flags from Node where id=?".to_string())?;
        Ok(SqlStatement {
            stmt,
            tok: FetchFlagsForRule,
        })
    }

    fn insert_trace_prepare(&self) -> Result<SqlStatement> {
        let stmt = self
            .prepare("INSERT INTO DYNIO (path, pid, gen, typ, childcnt) VALUES (?, ?, ?, ?, ?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertTrace,
        })
    }

    fn insert_monitored_prepare(&self) -> Result<SqlStatement> {
        let stmt = self
            .prepare("INSERT INTO MONITORED_FILES (name, generation_id, event) VALUES (?, ?, ?)")?;
        Ok(SqlStatement {
            stmt,
            tok: InsertMonitored,
        })
    }
    fn remove_monitored_by_id_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("DELETE from MONITORED_FILES where generation_id < ?")?;
        Ok(SqlStatement {
            stmt,
            tok: RemoveMonitoredById,
        })
    }

    fn remove_monitored_gen_files_prepare(&self) -> Result<SqlStatement> {
        let ftype = GenF;
        let stmt = self
            .prepare(&*format!(
                "DELETE from MONITORED_FILES where generation_id = ? and path in (\
            SELECT (DirPathBuf.name || '/' || Node.name) name from NODE inner join \
            DirPathBuf on (Node.dir = DirPathBuf.id) where (Node.type = {ftype} ) \
            and Node.id in (SELECT id from ModifyList)"
            ))
            .expect("unable to prepare stmt to fetch generated files in last build");
        Ok(SqlStatement {
            stmt,
            tok: RemoveMonitoredByPath,
        })
    }

    fn fetch_monitored_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT name from MONITORED_FILES where generation_id = ?")?;
        Ok(SqlStatement {
            stmt,
            tok: FetchMonitored,
        })
    }
    fn fetch_message_prepare(&self) -> Result<SqlStatement> {
        let stmt = self.prepare("SELECT message from MESSAGE where id = (SELECT (MAX(id)")?;
        Ok(SqlStatement {
            stmt,
            tok: FetchMessage,
        })
    }
}
impl LibSqlExec for SqlStatement<'_> {
    fn add_to_modify_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        assert_eq!(self.tok, AddToMod, "wrong token for update to modifylist");
        // statement ignores input if already present. So we call the method execute here rather than insert
        self.stmt.execute((id, rtype as u8))?;
        Ok(())
    }
    fn add_to_delete_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        assert_eq!(self.tok, AddToDel, "wrong token for update to deletelist");
        // statement ignores input if already present. So we call the method execute here rather than insert
        self.stmt.execute((id, rtype as u8))?;
        Ok(())
    }
    fn add_to_present_exec(&mut self, id: i64, rtype: RowType) -> Result<()> {
        assert_eq!(self.tok, AddToPres, "wrong token for update to presentlist");
        self.stmt.execute((id, rtype as u8))?;
        Ok(())
    }

    fn insert_dir_exec(&mut self, path_str: &str, dir: i64) -> Result<i64> {
        assert_eq!(self.tok, InsertDir, "wrong token for Insert dir");
        let id = self.stmt.insert([
            path_str,
            dir.to_string().as_str(),
            (RowType::Dir as u8).to_string().as_str(),
        ])?;
        Ok(id)
    }

    fn insert_dir_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()> {
        assert_eq!(
            self.tok, InsertDirAux,
            "wrong token for Insert Dir Into DirPathBuf"
        );
        let path_str = db_path_str(path);
        self.stmt.insert((id, path_str))?;
        Ok(())
    }

    fn insert_tup_aux_exec<P: AsRef<Path>>(&mut self, id: i64, path: P) -> Result<()> {
        assert_eq!(
            self.tok, InsertTupAux,
            "wrong token for Insert Tup Into TupPathBuf"
        );
        let path_str = db_path_str(path);
        self.stmt.insert((id, path_str))?;
        Ok(())
    }

    fn remove_tup_aux_exec(&mut self, p0: i64) -> Result<()> {
        assert_eq!(
            self.tok, RemoveTupAux,
            "wrong token for remove Tupfile from TupPathBuf"
        );
        self.stmt.execute([p0])?;
        Ok(())
    }
    fn remove_dir_aux_exec(&mut self, id: i64) -> Result<()> {
        assert_eq!(
            self.tok, RemoveDirAux,
            "wrong token for remove dir from DirPathBuf"
        );
        self.stmt.execute([id])?;
        Ok(())
    }

    fn insert_link(
        &mut self,
        from_id: i64,
        to_id: i64,
        is_sticky: bool,
        to_type: RowType,
    ) -> Result<()> {
        assert_eq!(self.tok, InsertLink, "wrong token for insert link");
        debug!("Normal link: {} -> {}", from_id, to_id);
        self.stmt
            .execute((from_id, to_id, is_sticky as u8, to_type as u8))?;
        Ok(())
    }

    fn insert_node_exec(&mut self, n: &Node) -> Result<i64> {
        assert_eq!(self.tok, InsertFile, "wrong token for Insert file");
        let r = self.stmt.insert((
            n.dirid,
            n.name.as_str(),
            n.mtime,
            *n.get_type() as u8,
            n.get_display_str(),
            n.get_flags(),
            n.get_srcid(),
        ))?;
        Ok(r)
    }

    fn insert_env_exec(&mut self, env: &str, val: &str) -> Result<i64> {
        assert_eq!(self.tok, InsertEnv, "wrong token for Insert env");
        let r = self.stmt.insert((env, val))?;
        Ok(r)
    }

    fn fetch_dirid<P: AsRef<Path>>(&mut self, p: P) -> Result<i64> {
        assert_eq!(self.tok, FindDirId, "wrong token for find dir");
        let path_str = db_path_str(p);
        let path_str = path_str.as_str();
        debug!("find dir id for :{}", path_str);
        let id = self.stmt.query_row([path_str], |r| r.get(0))?;
        Ok(id)
    }

    fn fetch_dirid_with_par<P: AsRef<Path>>(&mut self, p: P) -> Result<(i64, i64)> {
        assert_eq!(
            self.tok, FindDirIdWithPar,
            "wrong token for find dir with parent"
        );
        let path_str = db_path_str(p);
        let path_str = path_str.as_str();
        debug!("find dir id for :{}", path_str);
        let (id, dirid) = self
            .stmt
            .query_row([path_str], |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok((id, dirid))
    }

    fn fetch_group_id(&mut self, node_name: &str, dir: i64) -> Result<i64> {
        assert_eq!(self.tok, FindGroupId, "wrong token for fetch groupid");
        debug!("find group id for :{} at dir:{} ", node_name, dir);
        let v = self.stmt.query_row((dir, node_name), |r| {
            let v: i64 = r.get(0)?;
            Ok(v)
        })?;
        Ok(v)
    }

    fn fetch_node(&mut self, node_name: &str, dir: i64) -> Result<Node> {
        assert_eq!(self.tok, FindNode, "wrong token for fetch node");
        //log::info!("query for node:{:?}, {:?}", node_name, dir);

        let node = self.stmt.query_row((dir, node_name), make_node)?;
        Ok(node)
    }

    fn fetch_node_by_id(&mut self, i: i64) -> Result<Node> {
        assert_eq!(self.tok, FindNodeById, "wrong token for fetch node by id");
        let node = self.stmt.query_row((i,), make_node)?;
        Ok(node)
    }

    fn fetch_node_id(&mut self, node_name: &str, dir: i64) -> Result<i64> {
        assert_eq!(self.tok, FindNodeId, "wrong token for fetch node");
        debug!("query:{:?}, {:?}", node_name, dir);
        let nodeid = self.stmt.query_row((dir, node_name), |r| r.get(0))?;
        Ok(nodeid)
    }

    fn fetch_env_id(&mut self, env_name: &str) -> Result<(i64, String)> {
        assert_eq!(self.tok, FindEnvId, "wrong token for fetch env");
        debug!("query env:{:?}", env_name);
        let (nodeid, val) = self
            .stmt
            .query_row((env_name,), |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok((nodeid, val))
    }

    fn fetch_rule_nodes_by_dirid(&mut self, dir: i64) -> Result<Vec<Node>> {
        assert_eq!(self.tok, FindRuleNodes, "wrong token for fetch nodes");
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
        recursive: bool,
        mut f: F,
    ) -> Result<Vec<MatchingPath>>
    where
        F: FnMut(&String) -> Option<MatchingPath>,
        P: AsRef<Path>,
    {
        assert_eq!(self.tok, FindGlobNodes, "wrong token for fetch glob nodes");
        let mut path_str = db_path_str(base_path);
        if recursive {
            path_str.push('*')
        }
        //debug!("query glob:{:?}", gname.as_ref());
        let mut rows = self
            .stmt
            .query((gname.as_ref().to_string_lossy().as_ref(), path_str))?;
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
        assert_eq!(
            self.tok, FindParentRule,
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
        assert_eq!(self.tok, FindNodePath, "wrong token for fetch node path");
        let path_str: String = self.stmt.query_row([dirid], |r| r.get(0))?;
        Ok(Path::new(path_str.as_str()).join(name))
    }

    fn fetch_node_dir_path(&mut self, dirid: i64) -> Result<PathBuf> {
        assert_eq!(self.tok, FindNodePath, "wrong token for fetch node path");
        let path_str: String = self.stmt.query_row([dirid], |r| r.get(0))?;
        Ok(PathBuf::from(path_str.as_str()))
    }

    fn update_mtime_exec(&mut self, id: i64, mtime_ns: i64) -> Result<()> {
        assert_eq!(self.tok, UpdateMTime, "wrong token for update mtime");
        self.stmt.execute([mtime_ns, id])?;
        Ok(())
    }
    fn update_type_exec(&mut self, id: i64, row_type: RowType) -> Result<()> {
        assert_eq!(self.tok, UpdateType, "wrong token for update mtime");
        self.stmt.execute((row_type as u8, id))?;
        Ok(())
    }

    fn update_display_str(&mut self, id: i64, display_str: &str) -> Result<()> {
        assert_eq!(
            self.tok, UpdDisplayStr,
            "wrong token for update display string"
        );
        self.stmt.execute((display_str, id))?;
        Ok(())
    }

    fn update_flags_exec(&mut self, id: i64, flags: &str) -> Result<()> {
        assert_eq!(self.tok, UpdateFlags, "wrong token for update flags");
        self.stmt.execute((flags, id))?;
        Ok(())
    }
    fn update_srcid_exec(&mut self, id: i64, srcid: i64) -> Result<()> {
        assert_eq!(self.tok, UpdateSrcId, "wrong token for srcid update");
        self.stmt.execute([srcid, id])?;
        Ok(())
    }
    fn update_dirid_exec(&mut self, dirid: i64, id: i64) -> Result<()> {
        assert_eq!(self.tok, UpdateDirId, "wrong token for dirid update");
        self.stmt.execute([dirid, id])?;
        Ok(())
    }
    fn update_env_exec(&mut self, id: i64, val: &str) -> Result<()> {
        assert_eq!(
            self.tok, UpdateEnv,
            "wrong token for env display_str update"
        );
        self.stmt.execute((id, val))?;
        Ok(())
    }

    fn delete_exec(&mut self, id: i64) -> Result<()> {
        assert_eq!(self.tok, DeleteId, "wrong token for delete node");
        self.stmt.execute([id])?;
        Ok(())
    }

    fn mark_rule_outputs_deleted(&mut self, rule_id: i64) -> Result<()> {
        assert_eq!(
            self.tok, ImmediateDeps,
            "wrong token for mark immediate rule as deleted"
        );
        self.stmt.execute([rule_id])?;
        Ok(())
    }
    fn mark_rule_succeeded(&mut self, rule_id: i64) -> Result<()> {
        assert_eq!(
            self.tok, RuleSuccess,
            "wrong token for mark immediate rule as failed"
        );
        self.stmt.execute([rule_id, rule_id])?;
        Ok(())
    }
    fn fetch_dependent_tupfiles(&mut self, rule_id: i64) -> Result<Vec<Node>> {
        assert_eq!(self.tok, RuleDepRules, "wrong token for delete rule links");
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let n = make_tup_node(r)?;
            vs.push(n);
        }
        Ok(vs)
    }

    fn delete_normal_rule_links(&mut self, rule_id: i64) -> Result<()> {
        assert_eq!(
            self.tok, DeleteNormalRuleLinks,
            "wrong token for delete rule links"
        );
        self.stmt.execute([rule_id, rule_id])?;
        Ok(())
    }
    fn find_node_by_path<P: AsRef<Path>>(&mut self, dir_path: P, name: &str) -> Result<(i64, i64)> {
        assert_eq!(self.tok, FindNodeByPath, "wrong token for find by path");
        let dp = db_path_str(dir_path);
        let (id, dirid) = self.stmt.query_row(
            [name, dp.as_str()],
            |r: &Row| -> std::result::Result<(i64, i64), rusqlite::Error> {
                let id: i64 = r.get(0)?;
                let dirid: i64 = r.get(1)?;
                Ok((id, dirid))
            },
        )?;
        Ok((id, dirid))
    }

    fn fetch_io(&mut self, proc_id: i32) -> Result<Vec<(String, u8)>> {
        assert_eq!(self.tok, FetchIO, "wrong token for fetchio");
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
        assert_eq!(self.tok, FetchEnvsForRule, "wrong token for fetchenvs");
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let s: String = r.get(0)?;
            vs.push(s);
        }
        Ok(vs)
    }

    fn fetch_inputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        assert_eq!(
            self.tok, FetchInputsForRule,
            "wrong token for FetchInputsForRule"
        );
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let n = make_node(r)?;
            vs.push(n);
        }
        Ok(vs)
    }
    fn fetch_flags(&mut self, rule_id: i32) -> Result<Option<String>> {
        assert_eq!(
            self.tok, FetchFlagsForRule,
            "wrong token for FetchFlagsForRule"
        );
        let mut rows = self.stmt.query([rule_id])?;
        if let Some(r) = rows.next()? {
            let n: String = r.get(0)?;
            return Ok(Some(n));
        }
        Ok(None)
    }

    fn fetch_outputs(&mut self, rule_id: i32) -> Result<Vec<Node>> {
        assert_eq!(
            self.tok, FetchOutputsForRule,
            "wrong token for FetchOutputsForRule"
        );
        let mut rows = self.stmt.query([rule_id])?;
        let mut vs = Vec::new();
        while let Some(r) = rows.next()? {
            let n = make_node(r)?;
            vs.push(n);
        }
        Ok(vs)
    }

    fn insert_trace<P: AsRef<Path>>(
        &mut self,
        path: P,
        pid: i32,
        gen: u32,
        typ: u8,
        childcnt: i32,
    ) -> Result<()> {
        assert_eq!(self.tok, InsertTrace, "wrong token for insert trace");
        let filepath = db_path_str(path);
        self.stmt.insert((filepath, pid, gen, typ, childcnt))?;
        Ok(())
    }

    fn insert_monitored<P: AsRef<Path>>(
        &mut self,
        p: P,
        generation_id: i64,
        event: i32,
    ) -> Result<()> {
        assert_eq!(
            self.tok, InsertMonitored,
            "wrong token for insert monitored"
        );
        let path = db_path_str(p);
        self.stmt.insert((path, generation_id, event)).unwrap();
        Ok(())
    }
    fn fetch_monitored(&mut self, generation_id: i64) -> Result<Vec<(String, bool)>> {
        assert_eq!(self.tok, FetchMonitored, "wrong token for fetch monitored");
        let mut rows = self.stmt.query([generation_id]).unwrap();
        let mut vs = Vec::new();
        while let Some(row) = rows.next().unwrap() {
            let p: String = row.get(0).unwrap();
            let added: i32 = row.get(1).unwrap();
            vs.push((p, added == 1));
        }
        Ok(vs)
    }
    fn remove_monitored_by_path<P: AsRef<Path>>(&mut self, id: i64) -> Result<()> {
        assert_eq!(
            self.tok, RemoveMonitoredByPath,
            "wrong token for remove monitored"
        );
        //let path = db_path_str(p);
        self.stmt.execute((id,)).unwrap();
        Ok(())
    }
    fn remove_monitored_by_generation_id(&mut self, id: i64) -> Result<()> {
        assert_eq!(
            self.tok,
            StatementType::RemoveMonitoredById,
            "wrong token for remove monitored"
        );
        self.stmt.execute((id,)).unwrap();
        Ok(())
    }
}

pub(crate) fn db_path_str<P: AsRef<Path>>(p: P) -> String {
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

pub(crate) struct ConnWrapper<'a> {
    conn: &'a Connection,
}

impl<'a> ConnWrapper<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }
}

impl<'a> tupparser::decode::GroupInputs for ConnWrapper<'a> {
    fn get_group_paths(&self, group_name: &str, rule_id: i64, rule_dir: i64) -> Option<String>
    where
        Self: Sized,
    {
        let grptype = RowType::Grp as u8;
        let ftype = RowType::File as u8;
        let gftype = RowType::GenF as u8;
        // first fetch all the input groups to the given rule
        let group_name = if group_name.starts_with("<") {
            group_name.to_string()
        } else {
            format!("<{}>", group_name)
        };
        let mut dir_path = self.conn.fetch_node_path_prepare().expect(&*format!(
            "failed to prepare fetch node path statement for rule:{} in dir :{}",
            rule_id, rule_dir
        ));
        let rule_dir = dir_path.fetch_node_dir_path(rule_dir).expect(&*format!(
            "failed to fetch dir path for rule:{} in dir :{}",
            rule_id, rule_dir
        ));

        let mut fetch_input_groups_of_rule = self
            .conn
            .prepare(&*format!(
                "SELECT id, name from Node where type = {grptype} and id in\
         (SELECT from_id from NormalLink where to_id = ?)"
            ))
            .unwrap();
        let mut rows = fetch_input_groups_of_rule.query([rule_id]).unwrap();
        let mut find_group_id = |group_name: &str| {
            while let Some(row) = rows.next().unwrap() {
                let id: i64 = row.get(0).unwrap();
                let name: String = row.get(1).unwrap();
                if name.as_str().eq(group_name) {
                    return Some(id);
                }
            }
            return None;
        };
        find_group_id(group_name.as_str()).and_then(|grp_id| {
            let mut stmt = self.conn.prepare(&*format!("SELECT (DirPathBuf.name || '/' || Node.name) name from NODE inner join DirPathBuf on (Node.dir = DirPathBuf.id) where (Node.type = {ftype} or Node.type = {gftype}) and Node.id in (SELECT from_id from NormalLink  where to_id = ?)")).expect("unable to prepare stmt to fetch group input paths");
            let mut rows = stmt.query([grp_id]).expect("Unable to query for group input paths");
            let mut path = String::new();
            while let Some(row) = rows.next().unwrap() {
                let p: String = row.get(0).unwrap();
                path.push_str(diff_paths(p.as_str(), rule_dir.as_path()).unwrap_or(PathBuf::from(p)).to_string_lossy().to_string().as_str());
                path.push_str(" ");
            }
            path.pop();
            Some(path)
        })
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
        let tuptype = RowType::TupF as u8;
        let mut stmt = self.prepare(&*format!(
            "SELECT id,dir,name from TUPPATHBUF where id in \
        (SELECT id from ModifyList where type = {tuptype})"
        ))?;
        Self::for_tupid_and_path([], f, &mut stmt)?;
        Ok(())
    }
    fn for_tup_node_with_path<F>(&self, f: F) -> Result<()>
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
                AnyError::from(CallBackError::from(format!(
                    "unknown row type returned \
                 in  node query :{}",
                    e
                )))
            })?;
            mut_f(Node::new(i, dir, 0, name, rty))?;
        }
        Ok(())
    }

    fn rule_link(rule_id: i64, stmt: &mut Statement) -> Result<Vec<i64>> {
        let mut inputs = Vec::new();

        let rows = stmt.query_map([rule_id], |r| r.get(0))?;
        for row in rows {
            inputs.push(row?);
        }
        Ok(inputs)
    }

    fn for_each_link<F>(&mut self, f: F) -> Result<()>
    where
        F: FnMut(i32, i32) -> Result<()>,
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

    fn rules_to_run_no_target<F>(&self, mut f: F) -> Result<Vec<Node>>
    where
        F: FnMut(&Node) -> Result<Node>,
    {
        let rtype = RowType::Rule as u8;
        let stmt = &*format!("SELECT id, dir, name, display_str, flags, srcid  from Node where id in (SELECT id in ModifyList and type = {rtype})");
        let mut stmt = self.prepare(stmt)?;
        let mut rules = Vec::new();
        let mut rule_nodes = stmt.query([])?;
        while let Some(rule_node) = rule_nodes.next()? {
            let node = make_rule_node(rule_node)?;
            let node = f(&node)?;
            rules.push(node);
        }
        Ok(rules)
    }
}

impl MiscStatements for Connection {
    fn populate_delete_list(&self) -> Result<()> {
        let ftype = RowType::File as u8;
        let dtype = RowType::Dir as u8;
        let tup_type = RowType::TupF as u8;
        // add all files and directories to delete list that are not in the present list or modify list
        let mut stmt = self.prepare(
            &*format!("Insert  or IGNORE into DeleteList SELECT id, type from Node Where (type= {ftype} or type = {dtype} or type = {tup_type} ) and id NOT in( \
         SELECT id from PresentList)",
            ))?;
        stmt.execute([])?;

        Ok(())
    }

    fn first_containing(&self, name: &str, dir: i64) -> Result<(String, i64)> {
        let mut stmt = self.prepare(&*format!(
            "\
           WITH RECURSIVE DirectoryHierarchy AS (
    -- Anchor member: start with the current directory entry
    SELECT id, name, dir
    FROM Node
    WHERE id = {dir}

    UNION ALL

    -- Recursive member: continue with parent directories until dir = 0
    SELECT n.id, n.name, n.dir
    FROM Node n
    INNER JOIN DirectoryHierarchy dh ON n.id = dh.dir
    WHERE dh.dir != 0
)
SELECT f.name, f.dir
FROM DirectoryHierarchy dh
JOIN Node f ON f.dir = dh.id AND LOWER(f.name) = '{name}'
LIMIT 1;
     "
        ))?;

        let node = stmt.query_row([], |r| {
            let nodeid: String = r.get(0)?;
            let dirid = r.get(1)?;
            Ok((nodeid, dirid))
        })?;
        Ok(node)
    }

    // mark rules as Modified if any of its outputs are in the deletelist
    fn enrich_modified_list_with_outside_mods(&self) -> Result<()> {
        let rtype = RowType::Rule as u8;
        let mut stmt = self.prepare(
            &*format!("Insert or IGNORE into ModifyList SELECT id, {rtype} from Node where  type = {rtype} and id in \
            (SELECT from_id from NormalLink where to_id in (SELECT id from DeleteList  UNION SELECT id from ModifyList ) )"),
        )?;
        stmt.execute([])?;
        Ok(())
    }
    fn enrich_modified_list(&self) -> Result<()> {
        // add parent dirs to modify list if children of that directory are already in it
        let rtype = RowType::Rule as u8;
        //let grptype = RowType::Grp as u8;
        {
            // add parent dirs/glob to modify list if any child of that directory with matched glob pattern is in the delete list/modify list

            let ftype = RowType::File as u8;
            let gentype = RowType::GenF as u8;
            let globtype = RowType::Glob as u8;
            let tupf = RowType::TupF as u8;
            //stmt.execute([])?;
            // delete entries in modify list if they appear in delete list
            let mut stmt =
                self.prepare("DELETE from ModifyList where id in (SELECT id from DeleteList)")?;
            stmt.execute([])?;
            // mark glob patterns as modified if any of its inputs are in the deletelist or modified list
            // note that glob patterns are mapped to Tupfiles in  NormalLink table
            // which will then trigger parsing of these tupfiles
            let mut stmt = self.prepare(&*format!(
                "INSERT OR IGNORE INTO ModifyList
SELECT globPatterns.id, {globtype}
FROM Node AS globPatterns
WHERE globPatterns.type = {globtype}
AND EXISTS (
    SELECT 1
    FROM Node AS n
    WHERE
    n.id IN (
        SELECT id FROM DeleteList where type = {ftype} or type = {gentype}
        UNION
        SELECT id FROM ModifyList where type = {ftype} or type = {gentype}
    )
    AND n.dir = globPatterns.dir
    AND n.name GLOB globPatterns.name
)
"
            ))?;
            stmt.execute([])?;

            // remove rules in tupfile that was deleted.
            let mut stmt = self.prepare(&*format!(
                "INSERT or IGNORE into DeleteList SELECT id, {rtype} from Node where type = {rtype} and id in \
                (SELECT to_id from NormalLink where from_id in (SELECT id from DeleteList where type = {tupf} ) )")
            )?;
            stmt.execute([])?;

            // mark node as Modified if any of its inputs are in the deletelist or modified list
            let mut stmt = self.prepare(
                "Insert or IGNORE into ModifyList SELECT id, type from Node where id in \
            (SELECT to_id from NormalLink where from_id in (SELECT id from DeleteList  UNION SELECT id from ModifyList) )"
            )?;
            stmt.execute([])?;

            // mark node as Modified if it depends on nodes in deletelist or modified list
            let mut stmt = self.prepare(
                &*format!("Insert or IGNORE into ModifyList SELECT id, {rtype} from Node where  type = {rtype} and id in \
            (SELECT to_id from NormalLink where from_id in (SELECT id from DeleteList  UNION SELECT id from ModifyList ) )"),
            )?;
            stmt.execute([])?;

            // delete normal links with from / to id in deletelist
            let mut stmt = self.prepare("DELETE from NormalLink where from_id in (SELECT id from DeleteList) or to_id in (SELECT id from DeleteList)")?;
            stmt.execute([])?;

            // recursively mark all rules as modified if any of the parent rules are marked as modified
            // this is the closure operation.
            let mut stmt = self
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
        let mut stmt =
            self.prepare_cached("DELETE FROM PresentList WHERE id in (SELECT id from DeleteList)")?;
        stmt.execute([])?;
        Ok(())
    }

    fn prune_modified_list(&self) -> Result<()> {
        let mut stmt =
            self.prepare_cached("DELETE FROM ModifyList WHERE id in (SELECT id from DeleteList union SELECT id from SuccessList)")?;
        stmt.execute([])?;
        Ok(())
    }

    fn delete_nodes(&self) -> Result<()> {
        let mut stmt =
            self.prepare_cached("DELETE FROM Node WHERE id in (SELECT id from DeleteList)")?;
        stmt.execute([])?;
        let mut stmt = self.prepare_cached("DELETE from NormalLink where from_id in (SELECT id from DeleteList) or to_id in (SELECT id from DeleteList)")?;
        stmt.execute([])?;
        Ok(())
    }

    fn remove_modified_list(&self) -> Result<()> {
        let mut stmt = self.prepare_cached("DELETE FROM ModifyList")?;
        stmt.execute([])?;
        Ok(())
    }
    fn write_message(&self, message: &str) -> Result<()> {
        let mut stmt = self.prepare_cached("INSERT INTO MESSAGE (message) VALUES (?)")?;
        stmt.execute([message])?;
        Ok(())
    }

    fn read_message(&self, last_id: i64) -> Result<String> {
        let mut stmt =
            self.prepare_cached("SELECT id, message FROM MESSAGE ORDER BY id DESC LIMIT 1")?;
        let mut message_iter = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;

        message_iter
            .next()
            .ok_or(AnyError::Db(rusqlite::Error::QueryReturnedNoRows))
            .and_then(|message_row| {
                let (id, message): (i64, String) = message_row?;
                if id == last_id {
                    Err(AnyError::Db(rusqlite::Error::QueryReturnedNoRows))
                } else {
                    Ok(message)
                }
            })
    }
}

fn make_tup_node(row: &Row) -> rusqlite::Result<Node> {
    let i = row.get(0)?;
    let dir: i64 = row.get(1)?;
    let name: String = row.get(2)?;
    Ok(Node::new(i, dir, 0, name, RowType::TupF))
}

fn make_node(row: &Row) -> rusqlite::Result<Node> {
    let id: i64 = row.get(0)?;
    let dirid: i64 = row.get(1)?;
    let rtype: u8 = row.get(2)?;
    let name: String = row.get(3)?;
    let mtime: i64 = row.get(4)?;
    let rtype = RowType::try_from(rtype)
        .unwrap_or_else(|_| panic!("Invalid row type {} for node {}", rtype, name));
    Ok(Node::new(id, dirid, mtime, name, rtype))
}

fn make_rule_node(row: &Row) -> rusqlite::Result<Node> {
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

-- Node table queries
-- name: fetch_node_id_by_dir_and_name_inner?
-- Fetch the id of a node
-- # Parameters
-- param: dir : i64 - directory of the node as id
-- param: name : &str - name of the node
-- returns: i64
SELECT id
FROM LiveNode
where dir = :dir
  and name = :name;
-- <eos>

-- name: fetch_node_id_by_dir_and_name_raw_inner?
-- Fetch the id of a node (including deleted)
-- # Parameters
-- param: dir : i64 - directory of the node as id
-- param: name : &str - name of the node
-- returns: i64
SELECT id
FROM RawNode
where dir = :dir
  and name = :name;
-- <eos>

-- name: fetch_node_by_id_inner?
-- Fetch a node by id
-- # Parameters
-- param: id : i64 - id of the node
-- returns: Node
SELECT *
FROM LiveNode
where id = :id
LIMIT 1;
-- <eos>
-- name: fetch_node_name_by_id_inner?
-- Fetch a node by id
-- # Parameters
-- param: id : i64 - id of the node
-- returns: String
SELECT name
FROM LiveNode
where id = :id
LIMIT 1;
-- <eos>

-- name: fetch_node_by_dir_and_name_inner?
-- Fetch a node by directory id and name
-- # Parameters
-- param: dir : i64 - directory id
-- param: name : &str - name of the node
-- returns: Node
SELECT *
FROM LiveNode
where dir = :dir
  and name = :name;
-- <eos>

-- name: fetch_node_by_dir_and_name_raw_inner?
-- Fetch a node by directory id and name (including deleted)
-- # Parameters
-- param: dir : i64 - directory id
-- param: name : &str - name of the node
-- returns: Node
SELECT *
FROM RawNode
where dir = :dir
  and name = :name;
-- <eos>

-- name: fetch_rule_nodes_by_dir_inner&
-- Fetch the rule nodes by directory id
-- # Parameters
-- param: dir : i64 - directory id
SELECT id
FROM LiveNode
where dir = :dir
  and type = (SELECT type_index from NodeType where type = 'Rule');
-- <eos>

-- name: for_each_gen_file_inner&
-- Fetch all the nodes
SELECT n.id, n.dir, n.type, dpb.name || '/' || n.name, n.mtime_ns
from LiveNode n
         JOIN DirPathBuf dpb on n.dir = dpb.id
where n.type = (SELECT type_index from NodeType where type = 'GenF');
-- <eos>

-- name: fetch_node_flags_inner?
-- Fetch the flags of a node
-- # Parameters
-- param: node_id : i64 - id of the node
-- returns: String
SELECT flags
from LiveNode
where id = :node_id;
-- <eos>

-- name: fetch_parent_rule_of_node_inner?
-- Fetch the parent rule of a node
-- # Parameters
-- param: node_id : i64 - id of the node
-- returns: i64
SELECT srcid
from LiveNode
where id = :node_id;
-- <eos>

-- name: find_globs_at_dir?
-- Find the globs at a directory
-- # Parameters
-- param: dir_id : i64 - id of the directory
-- returns: i64
SELECT id
from LiveNode
where type = (SELECT type_index from NodeType where type = 'Glob')
  and dir = :dir_id;
-- <eos>

-- DirPathBuf table queries
-- name: fetch_dirid_by_path_inner?
-- Fetch the id of a directory by path
-- # Parameters
-- param: path : &str - path of the directory
-- returns: i64
SELECT id
FROM DirPathBuf
where name = :path;
-- <eos>

-- name: fetch_dirpath_by_dirid?
-- Fetch the directory path buffer by directory id
-- # Parameters
-- param: dir_id : i64 - directory id
-- returns: String
SELECT name
FROM DirPathBuf
where id = :dir_id;
-- <eos>

-- TupPathBuf table queries
-- name: for_each_modified_tupfile_inner&
-- Fetch the modified tup files
SELECT tpb.id, tpb.dir, tpb.name
from TupPathBuf tpb
         join ModifyList on tpb.id = ModifyList.id;
-- <eos>

-- name: for_each_tupnode_inner&
-- Fetch all the tup files
SELECT id, dir, name
from TupPathBuf;
-- <eos>

-- name: fetch_dependent_tup_files_by_rule_id&
-- Fetch the dependent tup file ids from rule_id
-- # Parameters
-- param: rule_id : i64 - id of the rule
SELECT id, dir, name
from TUPPATHBUF
where dir in
      (SELECT dir
       from Node
       where type = (SELECT type_index from NodeType where type = 'Rule')
         and id in
             (WITH RECURSIVE dependents(x) AS (SELECT :rule_id
                                               UNION
                                               SELECT to_id
                                               FROM LiveNormalLink
                                                        JOIN dependents ON LiveNormalLink.from_id = x)
              SELECT DISTINCT x
              FROM dependents));
-- <eos>

-- NodeSha table queries
-- name: fetch_node_sha_by_id?
-- Fetch the sha of a node by id
-- # Parameters
-- param: id : i64 - id of the node
-- returns: String
SELECT sha
from NodeSha
where id = :id;
-- <eos>

-- Rule related queries (joins)
-- name: for_each_rule_output_inner&
-- Fetch the outputs of a rule
-- # Parameters
-- param: rule_id : i64 - id of the rule
SELECT Node.id,
       Node.dir,
       Node.type,
       (DirPathBuf.name || '/' || Node.name) name,
       Node.mtime_ns                         mtime_ns
from LiveNode as Node
         inner join DirPathBuf on (Node.dir = DirPathBuf.id)
where Node.srcid = :rule_id
  and Node.type = (SELECT type_index from NodeType where type = 'GenF');
-- <eos>

-- name: for_each_rule_input_inner&
-- Fetch the file inputs of a rule or a group
-- # Parameters
-- param: node_id : i64 - id of the rule
SELECT Node.id   as                          id,
       Node.dir  as                          dir,
       Node.type as                          type,
       (DirPathBuf.name || '/' || Node.name) AS name,
       Node.mtime_ns                         AS mtime_ns
from LiveNode as Node
         join DirPathBuf on (Node.dir = DirPathBuf.id)
         join LiveNormalLink nl on (Node.id = nl.from_id)
where (Node.type in (SELECT type_index from NodeType where class = 'FILE_SYS'))
  and nl.to_id = :node_id;
-- <eos>

-- name: fetch_rule_input_matching_group_name_inner?
-- Fetch the groups which are inputs of a rule
-- # Parameters
-- param: rule_id : i64 - id of the rule
-- param: group: &str - name of the group
-- returns: i64
SELECT id
from LiveNode n
         JOIN LiveNormalLink nl on (n.id = nl.from_id)
where n.type in (SELECT type_index from NodeType where type = 'Group')
  and nl.to_id = :rule_id
  and n.name = :group
LIMIT 1;
-- <eos>

-- Complex queries with multiple joins
-- name: fetch_glob_matches_deep&
-- Fetch the nodes matching a glob node by tracking matches in the file system paths with dirs in GlobWatchDirs
-- # Parameters
-- param: glob_id : i64 - id of the glob node
SELECT n.id
FROM LiveNode as n
         JOIN DirPathBuf as dpb ON n.dir = dpb.id
         JOIN LiveNormalLink as gwd ON dpb.id = gwd.from_id
         JOIN LiveNode as n2 ON gwd.from_id = n2.id
WHERE n.name GLOB n2.name
  AND dpb.name GLOB n2.display_str
  AND gwd.to_id = :glob_id
  AND (n.type in (SELECT type_index from NodeType where class = 'FILE_SYS'));
-- <eos>

-- name: fetch_node_by_name_and_dirpath?
-- Fetch a node by name and directory path
-- # Parameters
-- param: name : &str - name of the node
-- param: dirpath : &str - path of the directory
-- returns: Node
SELECT *
FROM LiveNode n
         JOIN DirPathBuf dpb on dpb.id = n.dir
where n.name = :name
  and dpb.name = :dirpath
LIMIT 1;
-- <eos>

-- Other table queries
-- name: fetch_io_entries?
-- Fetch the io entries
-- # Parameters
-- param: pid : i32 - process id
SELECT path, pid, gen, typ
from DynIO
where pid = :pid;
-- <eos>

-- name: fetch_env_value?
-- Fetch value of env variable and its id
-- # Parameters
-- param: name: &str - env variable name
-- returns: (i64, String)
Select id, display_str
from LiveNode
where name = :name
  and dir = -2
LIMIT 1;
-- <eos>

-- name: fetch_env_deps?
-- Fetch the environment dependencies (as defined by export statements) of a rule  
-- # Parameters
-- param: rule_id : i64 - id of the rule
SELECT name
from LiveNode as n
         join LiveNormalLink as nl on n.id = nl.to_id
where nl.from_id = :rule_id
  and n.dir = -2;
-- <eos>

-- name: fetch_monitored_files_inner?
-- Fetch the monitored files
-- # Parameters
-- param: generation_id : i64 - generation id
SELECT name, event
from Monitored_Files
where generation_id = :generation_id;
-- <eos>

-- name: fetch_latest_message?
-- Fetch the latest message
-- # Parameters
-- returns: String
SELECT message
from Messages
order by id desc
limit 1;
-- <eos>

-- name: fetch_closest_parent_inner?
-- Fetch the closest parent directory entry with the given name from the given directory
-- # Parameters
-- param: name : &str - name of the directory entry
-- param: dir : i64 - directory id
-- returns: (i64, String)
WITH RECURSIVE parentDirectories as (
    -- anchor member: start with the current directory entry
    Select id, name, dir
    From LiveNode
    Where id = :dir

    Union All

    -- continue to find parent directories until dir = 0
    Select n.id, n.name, n.dir
    From LiveNode n
             Inner Join parentDirectories pd on n.id = pd.dir
    Where pd.dir != 0)
Select f.name, f.dir
From parentDirectories pd
         Join LiveNode f on f.dir = pd.id and f.name = :name
LIMIT 1;
-- <eos>

-- name: for_each_rule_to_run_no_targets_inner&
-- Fetch the rules to run with no targets
SELECT n.id, n.dir, n.name, n.display_str, n.flags, n.srcid
from LiveNode n
         JOIN ModifyList ML on n.id = ML.id
where ML.type = 1;
-- 1 is the  node type for a rule
-- <eos>

-- name: for_each_task_to_run_no_targets_inner&
-- Fetch the tasks to run with no targets
SELECT n.id, n.dir, n.name, n.display_str, n.flags, n.srcid
from LiveNode n
         JOIN ModifyList ML on n.id = ML.id
where ML.type = 10;
-- 10 is the  node type for a task
-- <eos>


-- name: for_each_link_inner&
-- Fetch the links
SELECT from_id, to_id
from LiveNormalLink;
-- <eos>

-- name: fetch_maybe_changed_globs_inner&
-- Fetch the globs with modified search directories
SELECT DISTINCT gwd.to_id
from LiveNormalLink as gwd
         JOIN ChangeList as cl on gwd.from_id = cl.id
where gwd.to_type = (SELECT type_index from NodeType where type = 'Glob');
-- <eos>


-- name: for_each_glob_match_inner&
-- Fetch glob matches within a subtree of directories with a given depth
-- # Parameters
-- param: glob_dir_id : i64 - id of the directory containing the glob
-- param: glob_depth : i64 - max depth of the directory from the root of the glob
-- param: glob_pattern : &str - glob pattern to match file names
-- param: glob_dir_pattern : &str - glob pattern to match directories
WITH RECURSIVE sub_tree AS (
    -- Base case: Start with the specified parent node (ensure it's a directory)
    SELECT id, dir, name, 1 AS depth
    FROM DIRPATHBUF
    WHERE id = :glob_dir_id -- Ensure the starting node is a folder
    UNION ALL

    -- Recursive step: Find children that are directories
    SELECT dt.id, dt.dir, dt.name, st.depth + 1 AS depth
    FROM DirPathBuf dt
             JOIN sub_tree st ON dt.dir = st.id
        AND (st.depth < :glob_depth))
SELECT DISTINCT  n.id, n.dir, n.type, st.name || '/' || n.name, n.mtime_ns
FROM LiveNode n
         JOIN sub_tree as st on n.dir = st.id
WHERE n.name GLOB :glob_pattern
  AND st.name GLOB :glob_dir_pattern
  AND (n.type in (SELECT type_index from NodeType where class = 'FILE_SYS'))
ORDER BY n.id;

-- <eos>
-- name: for_each_shallow_glob_match_inner&
-- Insert a directory into the glob watch list
-- # Parameters
-- param: glob_dir_id : i64 - id of the directory containing the glob
-- param: glob_pattern : &str - glob pattern to match file names
SELECT n.id, n.dir, n.type, st.name || '/' || n.name, n.mtime_ns
FROM LiveNode n
         join DIRPATHBUF as st on st.id = n.dir
WHERE st.id = :glob_dir_id
  and n.name GLOB :glob_pattern
  AND (n.type in (SELECT type_index from NodeType where class = 'FILE_SYS'))
ORDER BY n.id;
-- <eos>

-- name: for_each_subdirectory_inner&
-- Find immediate subdirectories of a directory
-- # Parameters
-- param: dir_id : i64 - id of the directory
SELECT id, name FROM LiveNode WHERE dir = :dir_id AND (type in (SELECT type_index from NodeType where type LIKE 'Dir%'));
-- <eos>

-- name: for_each_glob_dir_inner&
--  Find the directories within which glob searches would happen, this is needed for watching for changes
-- # Parameters
-- param: glob_dir_id : i64 - id of the directory containing the glob
-- param: glob_depth : i32 - max depth of the directory from the root of the glob
WITH RECURSIVE sub_tree AS (
    -- Base case: Start with the specified parent node (ensure it's a directory)
    SELECT id, dir, name, 1 AS depth
    FROM DIRPATHBUF
    WHERE id = :glob_dir_id -- Ensure the starting node is a folder
    UNION ALL

    -- Recursive step: Find children that are directories
    SELECT dt.id, dt.dir, dt.name, st.depth + 1 AS depth
    FROM DirPathBuf dt
             JOIN sub_tree st ON dt.dir = st.id
        AND (st.depth < :glob_depth))
SELECT name
FROM sub_tree;

-- <eos>


    
-- name: check_is_in_update_universe_inner?
-- Check if the given id is in the update universe
-- # Parameters
-- param: id : i64 - id of the node
-- returns: bool
SELECT EXISTS(SELECT 1 FROM TupfileEntities WHERE id = :id);
-- <eos>

-- name: has_rules_tasks_or_globs_inner?
-- Check if there are rules, tasks or globs in the database
-- returns: i64
SELECT 1
FROM Node
WHERE type IN (SELECT type_index FROM NodeType WHERE type IN ('Rule', 'Task', 'Glob'));
-- <eos>

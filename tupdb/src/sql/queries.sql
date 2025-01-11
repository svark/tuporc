
-- Node table queries
-- name: fetch_node_id?
-- Fetch the id of a node
-- # Parameters
-- param: dir : i64 - directory of the node as id
-- param: name : &str - name of the node
SELECT id FROM Node where dir=:dir and name=:name
/

-- name: fetch_node_by_id_inner?
-- Fetch a node by id
-- # Parameters
-- param: id : i64 - id of the node
SELECT * FROM Node where id=:id
/
-- name: fetch_node_name_by_id_inner?
-- Fetch a node by id
-- # Parameters
-- param: id : i64 - id of the node
SELECT name FROM Node where id=:id
/

-- name: fetch_node_by_dirid_and_name?
-- Fetch a node by directory id and name
-- # Parameters
-- param: dir : i64 - directory id
-- param: name : &str - name of the node
SELECT * FROM Node where dir=:dir and name=:name
/

-- name: fetch_nodeid_by_dirid_and_name?
-- Fetch the id of a node by directory id and name
-- # Parameters
-- param: dir : i64 - directory id
-- param: name : &str - name of the node
SELECT id FROM Node where dir=:dir and name=:name
/

--- name: fetch_rules_by_dirid_inner?
-- Fetch the rule nodes by directory id
-- # Parameters
-- param: dir : i64 - directory id
SELECT id FROM Node where dir=:dir and type = (SELECT type_index from NodeType where type='Rule')
/
                                             
                                             

-- name: fetch_node_flags_inner?
-- Fetch the flags of a node
-- # Parameters
-- param: node_id : i64 - id of the node
SELECT flags from Node where id=:node_id
/

-- name: fetch_parent_rule_of_node_inner?
-- Fetch the parent rule of a node
-- # Parameters
-- param: node_id : i64 - id of the node
SELECT srcid from Node where id=:node_id
/

-- name: find_globs_at_dir?
-- Find the globs at a directory
-- # Parameters
-- param: dir_id : i64 - id of the directory
SELECT id from Node where type = (SELECT type_index from NodeType where type='Glob') and dir = :dir_id
/

-- DirPathBuf table queries
-- name: fetch_dirid_by_path?
-- Fetch the id of a directory by path
-- # Parameters
-- param: path : &str - path of the directory
SELECT id FROM DirPathBuf where name=:path
/

-- name: fetch_dirid_and_parent_by_path?
-- Fetch the id of a directory and its parent by path
-- # Parameters
-- param: path : &str - path of the directory
SELECT id, dir FROM DirPathBuf where name=:path
/

-- name: fetch_node_by_path_inner?
-- Fetch a node by path
-- # Parameters
-- param: filename: &str  - name of the node
-- param: dir_path : &str - dir path of the node
SELECT Node.id, Node.dir from Node join DIRPATHBUF on Node.dir = DIRPATHBUF.id where DIRPATHBUF.name  = :dir_path and Node.name =:filename;

-- name: fetch_dirpath_by_dirid?
-- Fetch the directory path buffer by directory id
-- # Parameters
-- param: dir_id : i64 - directory id
SELECT name FROM DirPathBuf where id=:dir_id
/

-- TupPathBuf table queries
-- name: fetch_modified_tup_files?
-- Fetch the modified tup files
SELECT id, dir,name from TupPathBuf
join  ModifyList on TupPathBuf.id = ModifyList.id
where type = (SELECT type_index from NodeType where type='TupF')
/

-- name: fetch_all_tup_files?
-- Fetch all the tup files
SELECT id, dir, name from TupPathBuf
/

-- name: fetch_all_links?
-- Fetch all the links
SELECT from_id, to_id from NormalLink
/

-- name: fetch_dependent_tup_files?
-- Fetch the dependent tup file ids from rule_id
-- # Parameters
-- param: rule_id : i64 - id of the rule
SELECT id, dir, name from TUPPATHBUF where dir in
             (SELECT dir from Node where type = (SELECT type_index from NodeType where type='Rule') and id in
            (WITH RECURSIVE dependents(x) AS (
   SELECT :rule_id
   UNION
  SELECT to_id FROM NormalLink JOIN dependents ON NormalLink.from_id=x
)
SELECT DISTINCT x FROM dependents))
/

-- NodeSha table queries
-- name: fetch_node_sha_by_id?
-- Fetch the sha of a node by id
-- # Parameters
-- param: id : i64 - id of the node
SELECT sha from NodeSha where id=:id
/

-- Rule related queries (joins)
-- name: fetch_rule_outputs?
-- Fetch the outputs of a rule
-- # Parameters
-- param: rule_id : i64 - id of the rule
SELECT Node.id id, Node.dir dir, Node.type type,
         (DirPathBuf.name || '/' || Node.name) name, Node.mtime_ns mtime_ns from NODE 
         inner join DirPathBuf on (Node.dir = DirPathBuf.id)
         where  Node.srcid = :rule_id and Node.type = (SELECT type_index from NodeType where type='GenF')
/

-- name: fetch_node_input_files?
-- Fetch the file inputs of a rule or a group
-- # Parameters
-- param: node_id : i64 - id of the rule
SELECT Node.id as id, Node.dir as dir, Node.type as type,
        (DirPathBuf.name || '/' || Node.name) name, Node.mtime_ns mtime_ns
        from NODE 
        join DirPathBuf on (Node.dir = DirPathBuf.id) 
        join NormalLink nl on (Node.id = nl.from_id)
        where (Node.type in (SELECT type_index from NodeType where class='FILE_SYS'))
        and nl.to_id = :node_id
/

-- name: fetch_group_inputs_of_rule_matching?
-- Fetch the groups which are inputs of a rule
-- # Parameters
-- param: rule_id : i64 - id of the rule
-- group_name: &str - name of the group
SELECT id from Node n
JOIN NormalLink nl on (n.id = nl.from_id)
where n.type in (SELECT type_index from NodeType where type='Group') 
and nl.to_id = :rule_id and n.name = :group_name
/

-- Complex queries with multiple joins
-- name: glob_matches?
-- Fetch the nodes matching a glob node by tracking matches in the file system paths with dirs in GlobWatchDirs
-- # Parameters
-- param: glob_id : i64 - id of the glob node
SELECT n.id FROM Node as n
        JOIN DirPathBuf as dpb ON n.dir = dpb.id
        JOIN NormalLink as gwd ON dpb.id = gwd.from_id
        JOIN Node as n2 ON gwd.from_id = n2.id
        WHERE n.name GLOB n2.name AND dpb.name GLOB n2.display_str
        AND gwd.to_id = :glob_id
        AND (n.type in (SELECT type_index from NodeType where class='FILE_SYS'))
/

-- name: fetch_node_by_name_and_dirpath?
-- Fetch a node by name and directory path
-- # Parameters
-- param: name : &str - name of the node
-- param: dirpath : &str - path of the directory
SELECT * FROM Node n
         JOIN DirPathBuf dpb on dpb.id = n.dir
         where n.name=:name and dpb.name=:dirpath;
/

-- Other table queries
-- name: fetch_io_entries?
-- Fetch the io entries
-- # Parameters
-- param: pid : i32 - process id
SELECT path, pid, gen, typ from DynIO where pid = :pid;
/

--- name: fetch_env_value?
-- Fetch value of env variable and its id
-- # Parameters
-- param: name: &str - env variable name
Select id, display_str from node where name=:name and dir=-2;

-- name: fetch_env_deps?
-- Fetch the environment dependencies (as defined by export statements) of a rule  
-- # Parameters
-- param: rule_id : i64 - id of the rule
SELECT name from Node  as n
join NormalLink as nl on n.id = nl.to_id
where nl.from_id = :rule_id and n.dir = -2
/

-- name: fetch_monitored_files_inner?
-- Fetch the monitored files
-- # Parameters
-- param: generation_id : i64 - generation id
SELECT name, event from Monitored_Files where generation_id = :generation_id;
/

-- name: fetch_latest_message?
-- Fetch the latest message
SELECT message from Messages order by id desc limit 1;
/

-- Recursive queries
-- name: fetch_closest_parent_inner?
-- Fetch the closest parent directory entry with the given name from the given directory
-- # Parameters
-- param: name : &str - name of the directory entry
-- param: dir : i64 - directory id
WITH RECURSIVE parentDirectories as (
    -- anchor member: start with the current directory entry
    Select id, name, dir
    From node
    Where id = :dir

    Union All

    -- continue to find parent directories until dir = 0
    Select n.id, n.name, n.dir
    From node n
    Inner Join parentDirectories pd on n.id = pd.dir
    Where pd.dir != 0
) Select f.name, f.dir
From parentDirectories pd
Join node f on f.dir = pd.id and LOWER(f.name) = :name
Limit 1
/

-- name: fetch_rules_to_run_no_targets?
-- Fetch the rules to run with no targets
SELECT n.id, n.dir, n.name, n.display_str, n.flags, n.srcid  from Node n
JOIN ModifyList ML on n.id = ML.id
where ML.type = (SELECT id in NodeTYpe where type='Rule');
/

-- name: fetch_links?
-- Fetch the links
SELECT from_id, to_id from NormalLink
/

-- name: fetch_globs_with_modified_search_dirs?
-- Fetch the globs with modified search directories
SELECT DISTINCT gwd.to_id from NormalLink as gwd
    JOIN  ModifyList as ml on gwd.from_id = ml.id;
/


-- name: fetch_glob_matches_deep?
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
    SELECT st.id, st.dir, st.name, st.depth + 1 AS depth
    FROM DirPathBuf dt
    JOIN sub_tree st ON dt.dir = st.id
    AND (st.depth < :glob_depth)
)
SELECT n.id, n.dir, n.type, st.name || '/' || n.name, n.mtime_ns  FROM Node n
JOIN subtree as st on n.dir = st.id
WHERE n.name GLOB :glob_pattern AND st.name GLOB :glob_dir_pattern
        AND (n.type in (SELECT type_index from NodeType where class='FILE_SYS'))
ORDER BY  n.id;

/
-- name: fetch_glob_matches_shallow?
-- Insert a directory into the glob watch list
-- # Parameters
-- param: glob_dir_id : i64 - id of the directory containing the glob
-- param: glob_pattern : &str - glob pattern to match file names
SELECT n.id, n.dir, n.type, st.name || '/' || n.name, n.mtime_ns  FROM Node n
join DIRPATHBUF as st on st.id = n.dir
WHERE  st.id = :glob_dir_id and n.name GLOB :glob_pattern
        AND (n.type in (SELECT type_index from NodeType where class='FILE_SYS'))
ORDER BY  n.id;
/

-- name: fetch_glob_dirs_deep?
--  Find the directories within which glob searches would happen, this is needed for watching for changes
-- # Parameters
-- param: glob_dir_id : i64 - id of the directory containing the glob
-- param: glob_depth : i32 - max depth of the directory from the root of the glob
WITH RECURSIVE sub_tree AS (
    -- Base case: Start with the specified parent node (ensure it's a directory)
    SELECT id, dir, name 1 AS depth
    FROM DIRPATHBUF
    WHERE id = :glob_dir_id -- Ensure the starting node is a folder
    UNION ALL

    -- Recursive step: Find children that are directories
    SELECT dt.id, dt.dir, dt.name, st.depth + 1 AS depth
    FROM DirPathBuf dt
    JOIN sub_tree st ON dt.dir = st.id
    AND (st.depth < :glob_depth)
)
SELECT name FROM  subtree

/

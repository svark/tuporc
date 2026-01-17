-- name: delete_node_inner!
-- Delete a node from the database
-- # Parameters
-- param: id : i64 - id of the node to delete
DELETE
FROM Node
WHERE id = :id;
-- <eos>

-- name: delete_node_sha_inner!
-- Delete a node sha from the database
-- # Parameters
-- param: id : i64 - id of the node whose sha we want to delete
DELETE
FROM NodeSha
WHERE id = :id;
-- <eos>

-- name: delete_link_inner!
-- Delete a link from the database by from or to id of a rule
-- # Parameters
-- param: from_id : i64 - id of the node to delete
-- param: to_id : i64 - id of the node to delete
DELETE
from NormalLink
where from_id = :from_id
   or to_id = :to_id;
-- <eos>

-- name: delete_monitored_files_by_generation_id_inner!
-- Delete all monitored files with generation id less than the given generation id
-- # Parameters
-- param: generation_id : i64 - generation id
DELETE
from MONITORED_FILES
where generation_id < :generation_id;
-- <eos>

-- name: delete_from_dirpathbuf_inner!
-- Delete a directory from the directory path buffer
-- # Parameters
-- param: dir_id : i64 - id of the directory to delete
DELETE
from DirPathBuf
where id = :dir_id;
-- <eos>

-- name: delete_from_modifylist_inner!
-- Delete a glob from the modify list
-- # Parameters
-- param: id : i64 - id of the node to delete
DELETE
from ChangeList
where id = :id
  and change = 0;
-- <eos>

-- name: delete_from_normal_link!
-- Delete a link from the database if the from_id or the to_id is in ChangeList (change=1)
-- # Parameters
DELETE
FROM NormalLink
WHERE EXISTS (
    SELECT 1
    FROM ChangeList CL
    WHERE CL.change = 1
      AND (CL.id = NormalLink.from_id OR CL.id = NormalLink.to_id)
);
-- <eos>

-- name: delete_from_delete_list_inner!
-- Delete a node from the delete list
-- # Parameters
-- param: id : i64 - id of the node to delete
DELETE
from ChangeList
where id = :id
  and change = 1;
-- <eos>


-- name: prune_delete_list_of_present_inner!
-- Prune the delete list of PresentList
-- # Parameters
DELETE
from ChangeList
where EXISTS (SELECT 1 from PresentList as P where P.id = ChangeList.id)
  and change = 1;
-- <eos>

-- name: prune_delete_list_inner!
-- Prune the delete list
-- # Parameters
DELETE
from ChangeList where change = 1;
-- <eos>

-- name: delete_tupfile_entries_not_in_present_list_inner!
-- Delete all tupfile entries that are not in the present list
-- # Parameters
INSERT OR REPLACE INTO ChangeList (id, type, change)
SELECT t.id, t.type, 1
FROM TupfileEntities t
         LEFT JOIN PresentList p ON t.id = p.id
WHERE p.id IS NULL;
-- <eos>

-- name: enrich_delete_list_for_missing_dirs_inner!
-- Add all FILE_SYS nodes whose parent directory id does not exist to ChangeList (change=1)
-- This captures orphans created when a directory row was deleted without cascading to children.
INSERT OR REPLACE INTO ChangeList (id, type, change)
SELECT n.id, n.type, 1
FROM Node n
         LEFT JOIN Node d ON d.id = n.dir
         JOIN NodeType nt ON nt.type_index = n.type
WHERE d.id IS NULL
  AND nt.class = 'FILE_SYS'
-- do not mark the root row
  AND NOT (n.dir = 0 AND n.name = '.');
-- <eos>

-- name: enrich_delete_list_with_dir_dependents_inner!
-- Add nodes whose parent directory is marked for deletion into ChangeList (change=1)
INSERT OR REPLACE INTO ChangeList(id, type, change)
SELECT n.id,
       n.type,
       1
FROM Node n
         JOIN ChangeList d ON d.change = 1 AND n.dir = d.id;
-- <eos>
    
-- name: delete_orphaned_tupentries_inner!
-- Delete all nodes orphaned by deletion of their source tupfile or rule
-- # Parameters
insert or
REPLACE
into ChangeList(id, type, change)
SELECT n.id, n.type, 1
from Node n
         JOIN ChangeList dl on dl.change = 1 AND n.srcid = dl.id
where n.type = (SELECT type_index from NodeType where type = 'Rule')
  and dl.type = (SELECT type_index from NodeType where type = 'TupF');

-- <eos>
INSERT or
REPLACE
into ChangeList(id, type, change)
SELECT n.id, n.type, 1
from Node n
         JOIN ChangeList dl on dl.change = 1 AND dl.id = n.srcid
where n.type = (SELECT type_index from NodeTYpe where type = 'GenF')
  and dl.type = (SELECT type_index from NodeType where type = 'Rule');

-- <eos>

-- name: delete_nodes_inner!
-- Delete all nodes from the delete list
DELETE
from Node
where id in (SELECT id from ChangeList where change = 1);

-- <eos>
-- name: unmark_modified_inner!
-- Delete all entries from the ModifyList table
-- # Parameters
-- param: id : i64 - id of the node to delete
DELETE
from ChangeList
where change = 0
  and id = :id;

-- <eos>
-- name: delete_monitored_files_inner!
-- Delete all entries from the MonitoredFiles table
DELETE
from Monitored_Files;
-- <eos>

-- name: delete_messages_inner!
-- Delete all entries from the Messages table
DELETE
from Messages;
-- <eos>

-- name: delete_groups_with_no_ref_count_inner!
-- Count the number of tupfiles writing to-- <eos>reading from a group and delete the group if there is none
DELETE
FROM Node
WHERE type = (SELECT type_index FROM NodeType WHERE type = 'Group')
  AND NOT EXISTS (SELECT 1
                  FROM NormalLink
                  WHERE Node.id = NormalLink.to_id
                     or Node.id = NormalLink.from_id);
-- <eos>

-- name: delete_glob_with_no_ref_count_inner!
-- Count the number of tupfiles reading from a glob and delete the glob if the count is 0
DELETE
FROM Node
WHERE type = (SELECT type_index FROM NodeType WHERE type = 'Glob')
  AND NOT EXISTS (SELECT 1
                  FROM NormalLink
                  WHERE Node.id = NormalLink.from_id);
-- <eos>

-- name: mark_orphan_dirgen_nodes_inner!
-- Delete DirGen nodes that are orphaned (no non-DirGen descendants)
WITH RECURSIVE
    has_non_dirgen_descendant(id) AS (
        SELECT id FROM LiveNode WHERE type != (SELECT type_index FROM NodeType WHERE type = 'DirGen')
        UNION
        SELECT n.dir
        FROM LiveNode n
                 JOIN has_non_dirgen_descendant ON n.id = has_non_dirgen_descendant.id
        WHERE n.id != 0
    ),
    orphan_dirgens AS (
        SELECT ld.id, ld.type
        FROM LiveNode ld
        WHERE ld.type = (SELECT type_index FROM NodeType WHERE type = 'DirGen')
          AND NOT EXISTS (
                SELECT 1
                FROM has_non_dirgen_descendant h
                WHERE h.id = ld.id
            )
    )
Insert or REPLACE
into ChangeList(id, type, change)
SELECT id, type, 1 from orphan_dirgens;
-- <eos>

-- name: prune_modify_list_of_inputs_and_outputs_inner!
-- Prune the modify list of inputs and outputs of rules that are already in the modify list
-- Call this after the dependent rules have been added to the modify list or delete list
DELETE
FROM ChangeList
where type not in (SELECT type_index FROM NodeType WHERE type = 'Rule' or type = 'TupF');
-- <eos>

-- name: drop_tupfile_entities_table_inner!
-- Drop the temporary table to store tupfile entities
DROP TABLE TupfileEntities;
-- <eos>

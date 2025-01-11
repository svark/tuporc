--- name: delete_node_inner!
--- Delete a node from the database
--- # Parameters
--- param: id : i64 - id of the node to delete
DELETE FROM Node WHERE id=:id

/
--- name: delete_node_sha_inner!
--- Delete a node sha from the database
--- # Parameters
--- param: id : i64 - id of the node whose sha we want to delete
DELETE FROM NodeSha WHERE id=:id

/ 
--- name: delete_link_inner!
--- Delete a link from the database by from or to id of a rule
--- # Parameters
--- param: id : i64 - id of the node to delete
DELETE from NormalLink where from_id = :id or to_id  = :id                          
                          
/
--- name: delete_monitored_files_by_gen_id_inner!
--- Delete all monitored files with generation id less than the given generation id
--- # Parameters
--- param: generation_id : i64 - generation id
DELETE from MONITORED_FILES where generation_id < :generation_id
                                  
/

--- name: delete_from_dirpathbuf_inner!
---- Delete a directory from the directory path buffer
--- # Parameters
--- param: dir_id : i64 - id of the directory to delete
DELETE from DirPathBuf where id = :dir_id
/
--- name: delete_from_tuppathbuf_inner!
--- Delete a directory from the tup path buffer
--- # Parameters
--- param: dir_id : i64 - id of the directory to delete
Delete from TupPathBuf where id = :dir_id
/
--- name: delete_from_modifylist_inner!
--- Delete a glob from the modify list
--- # Parameters
--- param: id : i64 - id of the node to delete
DELETE from ChangeList where id = :id and is_delete = 0
/
--- name: delete_from_normal_link!
--- Delete a link from the database if the from_id or the to_id is in DeleteList
--- # Parameters
DELETE FROM NormalLink
WHERE EXISTS (
    SELECT 1
    FROM DeleteList
    WHERE DeleteList.id = NormalLink.from_id OR DeleteList.id = NormalLink.to_id
);
/
--- name: delete_from_delete_list_inner!
--- Delete a node from the delete list
--- # Parameters
--- param: id : i64 - id of the node to delete
DELETE from ChangeList where id = :id and is_delete = 1
/
--- name: prune_modify_list_of_success_inner!
--- Prune the modify list
--- # Parameters
DELETE from ChangeList where id not in (SELECT id from DeleteList UNION SELECT id from SuccessList)

/
--- name: prune_delete_list_of_present_inner!
--- Prune the delete list of PresentList
--- # Parameters
DELETE from TupfileEntities
where  EXISTS (SELECT 1 from PresentList where id = TupfileEntities.id);

/
--- name: delete_nodes_inner!
--- Delete all nodes from the delete list
DELETE from Node where id in (SELECT id from DeleteList);
/
--- name: delete_modify_list_inner!
--- Delete all entries from the ModifyList table
DELETE from ChangeList where is_delete = 0;
/
--- name: delete_monitored_files_inner!
--- Delete all entries from the MonitoredFiles table
DELETE from Monitored_Files;
/

--- name: delete_messages_inner!
--- Delete all entries from the Messages table
DELETE  from Messages
/

-- name: delete_groups_with_no_ref_count_inner!
-- Count the number of tupfiles writing to/reading from a group and delete the group if there is none
DELETE FROM Node
WHERE type = (SELECT type_index FROM NodeType WHERE type = 'Group')
  AND NOT EXISTS (
    SELECT 1
    FROM NormalLink
    WHERE  Node.id = NormalLink.to_id or Node.id = NormalLink.from_id
  );


/
-- name: delete_glob_with_no_ref_count_inner!
-- Count the number of tupfiles reading from a glob and delete the glob if the count is 0
DELETE FROM Node
WHERE type = (SELECT type_index FROM NodeType WHERE type = 'Glob')
  AND NOT EXISTS (
    SELECT 1
    FROM NormalLink
    WHERE Node.id = NormalLink.from_id
  );
/

-- name: prune_modify_list_of_inputs_and_outputs_inner!
-- Prune the modify list of inputs and outputs of rules that are already in the modify list
-- Call this after the dependent rules have been added to the modify list/delete list
DELETE FROM ChangeList where type <> (SELECT type_index FROM NodeType WHERE type = 'Rule' or type = 'TupF');

/


-- name: add_to_modify_list!
-- Add a node to the modify list
-- when node is already in delete list, keep it in delete list
-- # Parameters
-- param: id : i64 - id of the node to add
-- param: rtype : u8 - type of the node to add
INSERT or REPLACE into ChangeList(id, type, is_delete) Values (:id,:rtype, 0)
ON CONFLICT (id, type)  DO UPDATE SET is_delete =  MAX(0, ChangeList.is_delete);
;
-- <eos>
-- name: add_rules_with_changed_io_to_modify_list_inner!
-- Add all rules with modified (outside of build system) outputs to the modify list
BEGIN TRANSACTION;
Insert or REPLACE into ChangeList SELECT n.srcid,1 ,  -- rule
                                         0  -- change type 1 - ModifyList
                                  from Node n
join ChangeList c on n.id = c.id
where n.type = (SELECT type_index from NodeType where NodeType.type='GenF')
ON CONFLICT (id, type, is_delete) DO UPDATE  SET is_delete = MAX(0, ChangeList.is_delete);


INSERT or ignore  into ChangeList
SELECT nl.to_id, nl.to_type, 1  -- DeleteList sticky links trigger a delete on target rule if input to rule is in deletelist
from NormalLink  nl
JOIN ChangeList dl on dl.id = nl.from_id
WHERE
dl.is_delete = 1 and -- input is deleted
 nl.to_type = (SELECT  type_index from NodeType where type='Rule') -- rule
 and nl.issticky = 1;

INSERT or ignore  into ChangeList
SELECT nl.to_id, nl.to_type, 0 -- ModifyList non-sticky links trigger only a modify on target rule
from NormalLink  nl
JOIN ChangeList cl on cl.id = nl.from_id
WHERE
 nl.to_type = 1 -- rule type
 and nl.issticky = 0;
COMMIT ;
-- <eos>

-- name: add_to_present_list!
-- Add a node to the present list
-- # Parameters
-- param: id : i64 - id of the node to add
-- param: rtype: u8 - type of the node to add
INSERT or IGNORE into PresentList(id, type) Values (:id,:rtype);
-- <eos>

-- name: insert_link_inner!
-- Insert a link between two nodes
-- # Parameters
-- param: from_id : i64 - id of the node from which the link originates
-- param: to_id : i64 - id of the node to which the link points
-- param: issticky : u8 - whether the link is sticky
-- param: to_type : u8 - type of the node to which the link points
INSERT OR REPLACE into NormalLink (from_id, to_id, issticky, to_type)
    Values (:from_id,:to_id,:issticky, :to_type)
ON CONFLICT (from_id, to_id, issticky, to_type) DO NOTHING;
-- <eos>
-- name: insert_node_inner ->
-- Insert a node into the database
-- # Parameters
-- param: dir : i64 - id of the directory in which the node is located
-- param: name : &str - name of the node
-- param: mtime_ns : i64 - modification time of the node
-- param: rtype: u8 - type of the node
-- param: display_str : &str - string to display in the UI
-- param: flags : &str - flags for the node
-- param: srcid : i64 - id of the directory containing tupfile that generated the node
INSERT into Node (dir, name, mtime_ns, type, display_str, flags, srcid)
    Values (:dir,:name,:mtime_ns,:rtype,:display_str,:flags,:srcid);
-- <eos>

-- name: insert_env_var_inner ->
-- Insert an environment variable into the database
-- # Parameters
-- param: name : &str - name of the Env variable
-- param: value : &str - value of the Env variable
INSERT into Node (dir, name, type, display_str)
    VALUES (-2, :name, (SELECT type_index from NodeType where type='Env'), :value);

-- <eos>
-- name: update_mtime_ns!
-- Update the modification time of a node
-- # Parameters
-- param: mtime_ns : i64 - new modification time
-- param: id : i64 - id of the node to update
UPDATE Node Set mtime_ns = :mtime_ns where id = :id;
-- <eos>

-- name: update_type!
-- Update node type
-- # Parameters
-- param: rtype: u8 - new type of the node
-- param: id : i64 - id of the node to update
UPDATE Node Set type = :type where id = :id;

-- <eos>
-- name: insert_or_replace_node_sha!
-- Insert or replace the sha of a node
-- # Parameters
-- param: id : i64 - id of the node
-- param: sha : &str - sha of the node
INSERT or REPLACE into NodeSha (id, sha) Values (:id,:sha);

-- <eos>
-- name: update_node_dir!
-- Update the directory of a node
-- # Parameters
-- param: dir : i64 - new directory of the node
-- param: id : i64 - id of the node to update
UPDATE Node Set dir = :dir where id = :id;

-- <eos>
-- name: update_node_display_str!
-- Update the display string of a node
-- # Parameters
-- param: display_str : &str - new display string
-- param: id : i64 - id of the node to update
UPDATE Node Set display_str = :display_str where id = :id;

-- <eos>
-- name: update_node_flags!
-- Update the flags of a node
-- # Parameters
-- param: flags : &str - new flags
-- param: id : i64 - id of the node to update
UPDATE Node Set flags = :flags where id = :id;

-- <eos>
-- name: update_node_srcid!
-- Update the source id of a node
-- # Parameters
-- param: srcid : i64 - new source id
-- param: id : i64 - id of the node to update
UPDATE Node Set srcid = :srcid where id = :id;

-- <eos>
-- name: update_env_var!
-- Update the value of an environment variable
-- # Parameters
-- param: value : &str - new value of the environment variable
-- param: id : i64 - id of the environment variable to update
UPDATE Node Set display_str = :value where id = :id;

-- <eos>
-- name: insert_or_ignore_into_success_list!
-- Insert a rule into the success list
-- # Parameters
-- param: rule_id : i64 - id of the rule to insert
INSERT or IGNORE into SuccessList (id) SELECT :rule_id UNION ALL SELECT from_id from NormalLink where to_id=:rule_id;

-- <eos>
-- name: insert_glob_watch_dir!
-- Insert a directory into the glob watch list
-- # Parameters
-- param: dir_id : i64 - id of the directory to insert
-- param: glob_id : i64 - id of the glob to insert
INSERT or REPLACE INTO NormalLink (from_id, to_id, issticky, to_type) VALUES (:dir_id, :glob_id, 0,9)
ON CONFLICT (from_id, to_id, issticky, to_type) DO NOTHING;

-- <eos>

-- name: add_to_delete_list!
-- Add a node to the delete list
-- # Parameters
-- param: id : i64 - id of the node to add
-- param: rtype: u8 - type of the node to add
INSERT or REPLACE into ChangeList(id, type, is_delete) Values (:id,:rtype, 1)
ON CONFLICT (id, type, is_delete) DO NOTHING;
-- <eos>
-- name: delete_tupentries_in_deleted_tupfiles_inner!
-- Delete all nodes that are defined by tupfiles in delete list
-- # Parameters
insert or REPLACE into ChangeList(id, type, is_delete)
SELECT n.id, n.type, 1 from Node  n
JOIN DeleteList dl on  n.srcid = dl.id
where n.type = (SELECT type_index from NodeType where type='Rule')  and
dl.type = (SELECT type_index from NodeType where type='TupF')
ON CONFLICT (id, type, is_delete) DO NOTHING;

INSERT or REPLACE into ChangeList(id, type, is_delete)
SELECT n.id, n.type, 1
from Node n
JOIN DeleteList dl on dl.id = n.srcid
where n.type = (SELECT type_index from NodeTYpe where type='GenF')
and dl.type = (SELECT type_index from NodeType where type='Rule')
ON CONFLICT (id, type) DO NOTHING;

-- <eos>
-- name: add_not_present_to_delete_list_inner!
-- Add all deleted files and folders and env vars to the delete list
INSERT OR REPLACE INTO ChangeList (id, type, is_delete)
SELECT Node.id, Node.type, 1
FROM Node
LEFT JOIN PresentList ON Node.id = PresentList.id
INNER JOIN NodeType ON Node.type = NodeType.type_index
WHERE PresentList.id IS NULL
  AND (NodeType.class = 'FILE_SYS')
ON CONFLICT (id, type) DO NOTHING;
-- <eos>
-- name: create_tupfile_entities_table_inner!
-- Create a temporary table to store tupfile entities
CREATE TEMP TABLE TupfileEntities (id INTEGER PRIMARY KEY, type INTEGER);
-- <eos>

-- name: add_rules_and_outputs_of_tupfile_entities!
-- Add all rules of parsed tupfiles and their outputs to CurrentTupfiles
-- # Parameters
-- param: tupfile_id : i64- id of the tupfiles to add
INSERT INTO TupfileEntities (id, type)
SELECT id, type
FROM Node
WHERE srcid = :tupfile_id
  AND type = (SELECT type_index FROM NodeType WHERE type = 'Rule');

INSERT INTO TupfileEntities (id, type)
SELECT id, type
FROM Node
WHERE srcid IN (SELECT id FROM TupfileEntities);
-- <eos>

-- name: delete_tupfile_entries_not_in_present_list_inner!
-- Delete all tupfile entries that are not in the present list
-- # Parameters
INSERT INTO ChangeList (id, type, is_delete)
SELECT t.id, t.type, 1
FROM TupfileEntities t
LEFT JOIN PresentList p ON t.id = p.id
WHERE p.id IS NULL;
-- <eos>

-- name: add_message!
-- Add a message to the message table
-- # Parameters
-- param: message : &str - message to add
INSERT INTO MESSAGES (message) VALUES (:message);

-- <eos>
-- name: insert_trace_inner!
-- Insert a process trace into the DynIO table
-- # Parameters
-- param: path : &str - path of the trace
-- param: pid : i64 - process id
-- param: gen : i64 - generation id
-- param: typ : u8 - type of the trace
-- param: childcnt : i64 - number of children
INSERT INTO DYNIO (path, pid, gen, typ, childcnt) VALUES (:path, :pid, :gen, :typ, :childcnt);
-- <eos>


-- name: mark_dependent_tupfiles_inner!
-- Enrich the modify list with tupfiles that use modified globs
INSERT OR REPLACE INTO ChangeList (id, type, is_delete)
SELECT nl.to_id, nl.to_type, 0
from ChangeList cl
INNER JOIN NormalLink nl on nl.from_id = cl.id
WHERE nl.to_type = (SELECT type_index FROM NodeType WHERE type = 'TupF')
ON CONFLICT (id, type) DO NOTHING;

-- <eos>

-- name: mark_dependent_tupfiles_of_glob_inner!
-- Enrich the modify list with all rules that have modified outputs
-- # Parameters
-- param: glob_id : i64 - id of the glob
INSERT OR IGNORE INTO ChangeList (id, type, is_delete)
SELECT nl.to_id, nl.to_type, 0
from NormalLink nl where nl.from_id = :glob_id AND
nl.to_type = (SELECT type_index FROM NodeType WHERE type = 'TupF');

-- <eos>
-- name: prune_modifylist_of_non_rules_inner!
-- Prune the modify list of non-rules
DELETE FROM ChangeList WHERE type NOT IN
             (SELECT type_index FROM NodeType WHERE type = 'Rule' or type = 'TupF');
-- <eos>
-- name: mark_rules_depending_on_modified_groups_inner!
-- Enrich the modify list with all rules that depend on modified groups
INSERT OR IGNORE INTO ChangeList (id, type, is_delete)
SELECT nl.to_id, nl.to_type, 0
from ChangeList cl
INNER JOIN NormalLink nl on nl.from_id = cl.id
INNER JOIN NormalLink nl2  on nl.to_id = nl2.from_id
WHERE nl.to_type = (SELECT type_index FROM NodeType WHERE type = 'Group')
 and nl2.to_type = (SELECT type_index FROM NodeType WHERE type = 'Rule');
-- <eos>
-- name: push_remaining_tupfile_entries_to_deletelist_inner!
-- Push the remaining tupfile entries to the delete list
-- # Parameters
INSERT or IGNORE INTO ChangeList (id, type, is_delete)
    SELECT  id, type, 1 from TupfileEntities;
-- <eos>

-- name: insert_monitored_inner!
-- Insert a monitored file into the database
-- # Parameters
-- param: name : &str - name of the file
-- param: generation_id : i64 - generation id of the file
-- param: event : i32 - event that triggered the monitoring (added or modified=1, deleted = 0)
INSERT INTO MONITORED_FILES (name, generation_id, event) VALUES (:name, :generation_id, :event);
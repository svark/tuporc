CREATE TABLE IF NOT EXISTS Node
(
    id          INTEGER PRIMARY KEY not NULL, --- 0 is root
    dir         INTEGER             not NULL, -- node id of parent
    type        INTEGER             not NULL, -- type of node (Rule, GenF, File, Dir, TupF)
    name        VARCHAR(4096),                -- name of node (for file, dir, tupfile its basename) and for a rule the command line to execute
    mtime_ns    INTEGER DEFAULT 0,            -- mtime of file/dir
    display_str VARCHAR(4096),                -- string to display in the UI
    flags       VARCHAR(256),-- flags for the node
    srcid       INTEGER default -1,           -- id of the dir (which has Tupfile) that generated this node
    UNIQUE (dir, name)
); --- dir and name make a unique pair for a node

CREATE TABLE IF NOT EXISTS NodeType
(
    id    INTEGER PRIMARY KEY not NULL,
    type  CHAR(8),
    class VARCHAR(16)
);

DROP TABLE IF EXISTS DIRPATHBUF;

CREATE TABLE DIRPATHBUF AS
WITH RECURSIVE full_path(id, dir, name) AS
                   (VALUES (1, 0, '.')
                    UNION ALL
                    SELECT node.id as id, node.dir as dir, full_path.name || '/' || node.name as name
                    FROM node
                             JOIN full_path ON node.dir = full_path.id
                    where node.type = (SELECT N.type_index from NodeType N where N.type LIKE 'Dir%'))
SELECT id, dir, name
from full_path;

CREATE INDEX DIRPATHBUFNameIndex On DIRPATHBUF (name);
CREATE INDEX DIRPATHBUFIdIndex On DIRPATHBUF (id);

DROP TABLE IF EXISTS GRPPATHBUF;
CREATE TABLE GRPPATHBUF AS
SELECT node.id as id, DIRPATHBUF.name || '/' || node.name as Name
from node
         inner join DIRPATHBUF on
    (node.dir = DIRPATHBUF.id and node.type = (SELECT N.type_index from NodeType N where N.type = 'Group'));

DROP TABLE IF EXISTS TUPPATHBUF;
CREATE TABLE TUPPATHBUF AS
SELECT node.id as id, node.dir as dir, node.mtime_ns as mtime_ns, DIRPATHBUF.name || '/' || node.name as name
from Node
         inner join DIRPATHBUF ON
    (NODE.dir = DIRPATHBUF.id and node.type = (SELECT N.type_index from NodeType N where N.type = 'TupF'));


DROP TABLE IF EXISTS PresentList;
CREATE TABLE PresentList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
Create Table IF NOT EXISTS ChangeList
(
    id          INTEGER PRIMARY KEY not NULL,
    type        INTEGER,
    change_type INTEGER
);


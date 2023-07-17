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
    id   INTEGER PRIMARY KEY not NULL,
    type CHAR(4)
);

DROP TABLE IF EXISTS DIRPATHBUF;

CREATE TABLE DIRPATHBUF AS
WITH RECURSIVE full_path(id, name) AS
                   (VALUES (1, '.')
                    UNION ALL
                    SELECT node.id id, full_path.name || '/' || node.name name
                    FROM node
                             JOIN full_path ON node.dir = full_path.id
                    where node.type = (SELECT id from NodeType N where N.type LIKE 'Dir%'))
SELECT id, name
from full_path;
DROP TABLE IF EXISTS GRPPATHBUF;
CREATE TABLE GRPPATHBUF AS
SELECT node.id id, DIRPATHBUF.name || '/' || node.name Name
from node
         inner join DIRPATHBUF on
    (node.dir = DIRPATHBUF.id and node.type = (SELECT id from NodeType N where N.type = 'Grp'));

DROP TABLE IF EXISTS TUPPATHBUF;
CREATE TABLE TUPPATHBUF AS
SELECT node.id id, node.dir dir, node.mtime_ns mtime_ns, DIRPATHBUF.name || '/' || node.name name
from Node
         inner join DIRPATHBUF ON
    (NODE.dir = DIRPATHBUF.id and node.type = (SELECT id from NodeType N where N.type = 'TupF'));



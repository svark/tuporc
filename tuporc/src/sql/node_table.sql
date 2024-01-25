--- This file contains the schema for the database that persists between runs of tup

CREATE TABLE Node
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
);
--- dir and name make a unique pair for a node

--- NormalLink has links between tup files/rules/groups etc

CREATE TABLE NormalLink
(
    from_id  INTEGER,          -- id of the node that is the source of the link
    to_id    INTEGER,          -- id of the node that is the target of the link
    issticky INTEGER NOT NULL, -- is this a sticky link (statically determined) or not
    to_type  INTEGER,          -- type of the target node
    unique (from_id, to_id),   -- from_id and to_id make a unique pair for a link
    CHECK (issticky in (0, 1) )
);
-- issticky can only be 0 or 1


---  index the normal link table based on the to_id column
CREATE INDEX NormalLinkIdx ON NormalLink (to_id);


--- create a table for environment variables
CREATE TABLE Var
(
    id      INTEGER PRIMARY KEY,
    env_var VARCHAR
);


CREATE TABLE NodeType
(
    id   INTEGER PRIMARY KEY not NULL,
    type CHAR(4)
);
INSERT INTO NodeType (type, id)
VALUES ('File', 0),
       ('Rule', 1),
       ('Dir', 2),
       ('Env', 3),
       ('GenF', 4),
       ('TupF', 5),
       ('Grp', 6),
       ('DirGen', 7),
       ('Excl', 8),
       ('Glob', 9);


-- create a table containing a live list of files monitored under a root directory based on presence of keyword in the file
CREATE TABLE MONITORED_FILES
(
    id INTEGER PRIMARY KEY not NULL,
    name VARCHAR(4096) not NULL,
    event INTEGER not NULL,
    generation_id INTEGER not NULL
);
CREATE TABLE MESSAGES (id INTEGER PRIMARY KEY not NULL, message CHAR(8) not NULL);
--- following is so that some statements can be prepare in NodeStatement's constructor, even though they are not used
--- Actual construction of the table happens with with_recursive statement in ./dirpathbuf_temptable.sql
CREATE TABLE IF NOT EXISTS DIRPATHBUF (id INTEGER, dir INTEGER, name VARCHAR(4096));

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 10000;
---  mmap_size is the maximum amount of memory that SQLite is allowed to use for memory-mapped I/O.
PRAGMA mmap_size = 20000000000;
PRAGMA threads = 4;
--- PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 10000;
PRAGMA encoding = "UTF-8";
PRAGMA page_size = 4096;
PRAGMA auto_vacuum = INCREMENTAL;
PRAGMA incremental_vacuum(100);
PRAGMA wal_autocheckpoint = 1000;
PRAGMA wal_checkpoint(TRUNCATE);

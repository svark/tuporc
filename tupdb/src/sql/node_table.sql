--- This file contains the schema for the database that persists between runs of tup
CREATE TABLE Node
(
    id          INTEGER PRIMARY KEY  autoincrement not NULL, --- 0 is root
    dir         INTEGER             not NULL, -- node id of parent directory
    type        INTEGER             not NULL, -- type of node (Rule, GenF, File, Dir, TupF, etc)
    name        VARCHAR(4096),                -- name of node (for file, dir, tupfile its basename) and for a rule the command line to execute
    mtime_ns    INTEGER DEFAULT 0,            -- mtime of file/dir
    display_str VARCHAR(4096),                -- string to display in the UI
    flags       VARCHAR(256),-- flags for the node
    srcid       INTEGER default -1,           -- id of the dir (which has Tupfile) that generated this node
    UNIQUE (dir, name) --- dir and name make a unique pair for a node
);
Create UNIQUE INDEX NodeIdIndex on Node(id);
Create INDEX NodeSrcIdIndex on Node(srcid);
Create UNIQUE INDEX NodeDirNameIndex on Node(dir, name);

--- NormalLink has links between tup files/rules/groups etc
CREATE TABLE NormalLink
(
    from_id  INTEGER,          -- id of the node that is the source of the link
    to_id    INTEGER,          -- id of the node that is the target of the link
    issticky INTEGER NOT NULL, -- is this a sticky link (statically determined) or not
    to_type  INTEGER,          -- type of the target node
    unique (from_id, to_id),   -- from_id and to_id make a unique pair for a link
	FOREIGN KEY (from_id) references Node(id) on DELETE CASCADE,
    FOREIGN KEY (to_id) references Node(id) on DELETE CASCADE,
    CHECK (issticky in (0, 1) ) -- issticky can only be 0 or 1 indicating if this is hard link where delete should cascade
);



---  index the normal link table based on the to_id column
CREATE INDEX NormalLinkDstIdx ON NormalLink (to_id);
CREATE INDEX NormalLinkSrcIdx ON NormalLink (from_id);
CREATE INDEX NormalLinkTypeIdx ON NormalLink (to_type);



CREATE TABLE NodeType
(
    type_index   INTEGER PRIMARY KEY not NULL,
    type CHAR(8),
    class VARCHAR(16)
);
INSERT INTO NodeType (type, type_index, class)
VALUES ('File', 0, 'FILE_SYS'),
       ('Rule', 1, 'TUPLANG'),
       ('Dir', 2, 'FILE_SYS'),
       ('Env', 3, 'ENV'),
       ('GenF', 4, 'FILE_SYS'),
       ('TupF', 5, 'FILE_SYS'),
       ('Group', 6, 'TUPLANG'),
       ('DirGen', 7, 'FILE_SYS'),
       ('Excl', 8, 'TUPLANG'),
       ('Glob', 9, 'TUPLANG'),
       ('Task', 10, 'TUPLANG');


Create Table IF NOT EXISTS ChangeList
(
    id   integer PRIMARY KEY not null,
    type integer,
    is_delete integer,
    UNIQUE (id) ,
    FOREIGN KEY (id) references Node(id) on DELETE CASCADE,
    CHECK (is_delete in (0, 1) )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ChangeList_id ON ChangeList(id);

CREATE TABLE NODESHA
(
    id INTEGER PRIMARY KEY  not null,
    sha VARCHAR(256) not null,
    UNIQUE (id),
	FOREIGN KEY (id) references Node(id) on DELETE CASCADE

);

-- create a table containing a live list of files monitored under a root directory based on presence of keyword in the file
CREATE TABLE MONITORED_FILES
(
    id INTEGER PRIMARY KEY AUTOINCREMENT  not NULL,
    name VARCHAR(4096) not NULL,
    event INTEGER not NULL,
    generation_id INTEGER not NULL
);

CREATE TABLE MESSAGES (id INTEGER PRIMARY KEY AUTOINCREMENT not NULL, message CHAR(8) not NULL);
--- following is so that some statements can be prepared in NodeStatement's constructor, even though they are not used
--- Actual construction of the table happens with with_recursive statement in ./dirpathbuf_temptable.sql
CREATE TABLE IF NOT EXISTS DIRPATHBUF (id INTEGER, dir INTEGER, name VARCHAR(4096));
CREATE VIEW ModifyList AS
    SELECT id, type from ChangeList where is_delete = 0;
CREATE VIEW DeleteList AS
    SELECT id, type from ChangeList where is_delete = 1;

PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 10000;
---  mmap_size is the maximum amount of memory that SQLite is allowed to use for memory-mapped I/O.
PRAGMA mmap_size = 20000000000;
PRAGMA threads = 4;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 10000;
PRAGMA encoding ='UTF-8';
PRAGMA page_size = 4096;
PRAGMA auto_vacuum = INCREMENTAL;
PRAGMA incremental_vacuum(100);
PRAGMA wal_autocheckpoint = 1000;
PRAGMA wal_checkpoint(TRUNCATE);

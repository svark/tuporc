--- This file contains the schema for the database that persists between runs of tup
CREATE TABLE Node
(
    id          INTEGER PRIMARY KEY autoincrement not NULL, --- 0 is root
    dir         INTEGER                           not NULL, -- node id of parent directory
    type        INTEGER                           not NULL, -- type of node (Rule, GenF, File, Dir, TupF, etc)
    name        VARCHAR(4096),                              -- name of node (for file, dir, tupfile its basename) and for a rule the command line to execute
    mtime_ns    INTEGER DEFAULT 0,                          -- mtime of file/dir
    display_str VARCHAR(4096),                              -- string to display in the UI
    flags       VARCHAR(256),-- flags for the node
    srcid       INTEGER default NULL,                       -- id of the dir (which has Tupfile) that generated this node; NULL/-1 means non-generated or unknown
    FOREIGN KEY (srcid) references Node (id) ON DELETE SET NULL,
    UNIQUE (dir, name)                                      --- dir and name make a unique pair for a node
);
--- Indices for the Node table
Create UNIQUE INDEX NodeIdIndex on Node (id);
Create INDEX NodeSrcIdIndex on Node (srcid);
Create UNIQUE INDEX NodeDirNameIndex on Node (dir, name);

--- NormalLink has links between tup files/rules/groups etc
CREATE TABLE NormalLink
(
    from_id   INTEGER,           -- id of the node that is the source of the link
    to_id     INTEGER,           -- id of the node that is the target of the link
    is_sticky INTEGER NOT NULL,  -- is this a sticky link (statically determined) or not
    to_type   INTEGER,           -- type of the target node
    unique (from_id, to_id),     -- from_id and to_id make a unique pair for a link
    FOREIGN KEY (from_id) references Node (id) on DELETE CASCADE,
    FOREIGN KEY (to_id) references Node (id) on DELETE CASCADE,
    CHECK (is_sticky in (0, 1) ) -- is_sticky can only be 0 or 1 indicating if this is hard link where delete should cascade
);


---  index the normal link table based on the to_id column
CREATE INDEX NormalLinkDstIdx ON NormalLink (to_id);
CREATE INDEX NormalLinkSrcIdx ON NormalLink (from_id);
CREATE INDEX NormalLinkTypeIdx ON NormalLink (to_type);


--- NodeType holds the type of the node and the class it belongs to and its encoding 0-10 that is stored in Node table
CREATE TABLE NodeType
(
    type_index INTEGER PRIMARY KEY not NULL,
    type       CHAR(8),
    class      VARCHAR(16)
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


--- ChangeList holds the list of nodes that have been modified or deleted
Create Table IF NOT EXISTS ChangeList
(
    id        integer PRIMARY KEY not null,
    type      integer             not null,
    is_delete integer             not null,
    UNIQUE (id),
    FOREIGN KEY (id) references Node (id) on DELETE CASCADE,
    CHECK (is_delete in (0, 1) )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_ChangeList_id ON ChangeList (id);

--- NODESHA holds the sha of the node that are involved in the build
CREATE TABLE NODESHA
(
    id  INTEGER PRIMARY KEY not null,
    sha VARCHAR(256)        not null,
    UNIQUE (id),
    FOREIGN KEY (id) references Node (id) on DELETE CASCADE

);

-- create a table containing a live list of files monitored under a root directory based on presence of keyword in the file
CREATE TABLE MONITORED_FILES
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT not NULL,
    name          VARCHAR(4096)                     not NULL,
    event         INTEGER                           not NULL,
    generation_id INTEGER                           not NULL
);

-- create a table to store messages that are generated during the build
CREATE TABLE MESSAGES
(
    id      INTEGER PRIMARY KEY AUTOINCREMENT not NULL,
    message CHAR(8)                           not NULL
);

-- RunStatus tracks the last parse/build run status.
CREATE TABLE IF NOT EXISTS RunStatus
(
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    phase  TEXT,
    status TEXT,
    ts     INTEGER
);
-- Setup ModifyList view
CREATE VIEW ModifyList AS
SELECT id, type
from ChangeList
where is_delete = 0;
--- Setup deletelist view
CREATE VIEW DeleteList AS
SELECT id, type
from ChangeList
where is_delete = 1;

-- Persisted list of currently present filesystem nodes (rebuilt each scan/parse).
CREATE TABLE IF NOT EXISTS PresentList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_PresentList_id ON PresentList(id);

-- Raw views (no filtering) for scan-time queries.
CREATE VIEW RawNode AS
SELECT *
FROM Node;

CREATE VIEW RawNormalLink AS
SELECT from_id, to_id, is_sticky, to_type
FROM NormalLink;

-- LiveNode/LiveNormalLink filter out rows marked for deletion in ChangeList.
CREATE VIEW LiveNode AS
SELECT *
FROM Node n
WHERE NOT EXISTS (SELECT 1 FROM ChangeList dl WHERE dl.id = n.id AND dl.is_delete = 1);

CREATE VIEW LiveNormalLink AS
SELECT nl.from_id, nl.to_id, nl.is_sticky, nl.to_type
FROM NormalLink nl
WHERE NOT EXISTS (SELECT 1 FROM ChangeList d1 WHERE d1.id = nl.from_id AND d1.is_delete = 1)
  AND NOT EXISTS (SELECT 1 FROM ChangeList d2 WHERE d2.id = nl.to_id AND d2.is_delete = 1);
--- Pragma settings for the database
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA temp_store = MEMORY;
PRAGMA cache_size = 20000;

---  mmap_size is the maximum amount of memory that SQLite is allowed to use for memory-mapped I/O.
PRAGMA mmap_size = 21400000000;
PRAGMA threads = 4;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 10000;
PRAGMA encoding ='UTF-8';
PRAGMA page_size = 4096;
PRAGMA auto_vacuum = INCREMENTAL;
PRAGMA incremental_vacuum(100);
PRAGMA wal_autocheckpoint = 1000;
PRAGMA wal_checkpoint(TRUNCATE);

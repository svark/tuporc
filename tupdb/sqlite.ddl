CREATE TABLE Node
(
    id          INTEGER PRIMARY KEY autoincrement not NULL, --- 0 is root
    dir         INTEGER                           not NULL, -- node id of parent directory
    type        INTEGER                           not NULL, -- type of node (Rule, GenF, File, Dir, TupF, etc)
    name        VARCHAR(4096),                              -- name of node (for file, dir, tupfile its basename) and for a rule the command line to execute
    mtime_ns    INTEGER DEFAULT 0,                          -- mtime of file/dir
    display_str VARCHAR(4096),                              -- string to display in the UI
    flags       VARCHAR(256),-- flags for the node
    srcid       INTEGER default NULL,                         -- id of the dir (which has Tupfile) that generated this node
    FOREIGN KEY (srcid) references Node (id) ON DELETE SET NULL,
    UNIQUE (dir, name)                                      --- dir and name make a unique pair for a node
);
CREATE TABLE sqlite_sequence
(
    name,
    seq
);
CREATE UNIQUE INDEX NodeIdIndex on Node (id);
CREATE INDEX NodeSrcIdIndex on Node (srcid);
CREATE UNIQUE INDEX NodeDirNameIndex on Node (dir, name);
CREATE TABLE NormalLink
(
    from_id  INTEGER,           -- id of the node that is the source of the link
    to_id    INTEGER,           -- id of the node that is the target of the link
    is_sticky INTEGER NOT NULL,  -- is this a sticky link (statically determined) or not
    to_type  INTEGER,           -- type of the target node
    srcid    INTEGER default NULL,  -- id of Tupfile that generated this link; NULL/-1 means non-generated or unknown
    unique (from_id, to_id),    -- from_id and to_id make a unique pair for a link
    FOREIGN KEY (from_id) references Node (id) on DELETE CASCADE,
    FOREIGN KEY (to_id) references Node (id) on DELETE CASCADE,
    CHECK (is_sticky in (0, 1) ) -- is_sticky can only be 0 or 1 indicating if this is hard link where delete should cascade
);
CREATE INDEX NormalLinkDstIdx ON NormalLink (to_id);
CREATE INDEX NormalLinkSrcIdx ON NormalLink (from_id);
CREATE INDEX NormalLinkTypeIdx ON NormalLink (to_type);
CREATE INDEX NormalLinkSrcIdIdx ON NormalLink (srcid);
CREATE TABLE NodeType
(
    type_index INTEGER PRIMARY KEY not NULL,
    type       CHAR(8),
    class      VARCHAR(16)
);
CREATE TABLE ChangeList
(
    id        integer PRIMARY KEY not null,
    type      integer             not null,
    change integer             not null,
    UNIQUE (id),
    FOREIGN KEY (id) references Node (id) on DELETE CASCADE,
    CHECK (change in (0, 1) )
);
CREATE UNIQUE INDEX idx_ChangeList_id ON ChangeList (id);
CREATE TABLE NODESHA
(
    id  INTEGER PRIMARY KEY not null,
    sha VARCHAR(256)        not null,
    UNIQUE (id),
    FOREIGN KEY (id) references Node (id) on DELETE CASCADE
);
CREATE TABLE MONITORED_FILES
(
    id            INTEGER PRIMARY KEY AUTOINCREMENT not NULL,
    name          VARCHAR(4096)                     not NULL,
    event         INTEGER                           not NULL,
    generation_id INTEGER                           not NULL
);
CREATE TABLE MESSAGES
(
    id      INTEGER PRIMARY KEY not NULL,
    message CHAR(8)             not NULL
);
CREATE TABLE DIRPATHBUF
(
    id   INTEGER,
    dir  INTEGER,
    name VARCHAR(4096)
);
CREATE TEMP TABLE PresentList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
CREATE UNIQUE INDEX idx_PresentList_id ON PresentList (id);
CREATE TABLE SuccessList
(
    id INTEGER PRIMARY KEY not NULL
);
-- LiveNode/LiveNormalLink filter out rows marked for deletion in ChangeList.
CREATE VIEW LiveNode AS
SELECT *
FROM Node n
WHERE NOT EXISTS (SELECT 1 FROM ChangeList dl WHERE dl.id = n.id AND dl.change = 1);

CREATE VIEW LiveNormalLink AS
SELECT nl.from_id, nl.to_id, nl.is_sticky, nl.to_type, nl.srcid
FROM NormalLink nl
WHERE NOT EXISTS (SELECT 1 FROM ChangeList d1 WHERE d1.id = nl.from_id AND d1.change = 1)
  AND NOT EXISTS (SELECT 1 FROM ChangeList d2 WHERE d2.id = nl.to_id AND d2.change = 1);

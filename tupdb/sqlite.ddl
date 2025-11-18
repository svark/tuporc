CREATE TABLE Node
(
    id          INTEGER PRIMARY KEY autoincrement not NULL, --- 0 is root
    dir         INTEGER                           not NULL, -- node id of parent directory
    type        INTEGER                           not NULL, -- type of node (Rule, GenF, File, Dir, TupF, etc)
    name        VARCHAR(4096),                              -- name of node (for file, dir, tupfile its basename) and for a rule the command line to execute
    mtime_ns    INTEGER DEFAULT 0,                          -- mtime of file/dir
    display_str VARCHAR(4096),                              -- string to display in the UI
    flags       VARCHAR(256),-- flags for the node
    srcid       INTEGER default -1,                         -- id of the dir (which has Tupfile) that generated this node
    UNIQUE (dir, name)                                      --- dir and name make a unique pair for a node
);
CREATE TABLE sqlite_sequence
(
    name,
    seq
);
CREATE UNIQUE INDEX NodeIdIndex on Node (id)
CREATE INDEX NodeSrcIdIndex on Node (srcid)
CREATE UNIQUE INDEX NodeDirNameIndex on Node (dir, name)
CREATE TABLE NormalLink
(
    from_id  INTEGER,           -- id of the node that is the source of the link
    to_id    INTEGER,           -- id of the node that is the target of the link
    issticky INTEGER NOT NULL,  -- is this a sticky link (statically determined) or not
    to_type  INTEGER,           -- type of the target node
    unique (from_id, to_id),    -- from_id and to_id make a unique pair for a link
    FOREIGN KEY (from_id) references Node (id) on DELETE CASCADE,
    FOREIGN KEY (to_id) references Node (id) on DELETE CASCADE,
    CHECK (issticky in (0, 1) ) -- issticky can only be 0 or 1 indicating if this is hard link where delete should cascade
)
CREATE INDEX NormalLinkDstIdx ON NormalLink (to_id)
CREATE INDEX NormalLinkSrcIdx ON NormalLink (from_id)
CREATE INDEX NormalLinkTypeIdx ON NormalLink (to_type)
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
    is_delete integer             not null,
    UNIQUE (id),
    FOREIGN KEY (id) references Node (id) on DELETE CASCADE,
    CHECK (is_delete in (0, 1) )
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
CREATE TABLE TupfileEntities
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);

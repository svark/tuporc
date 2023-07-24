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
       ('Glob', 9)



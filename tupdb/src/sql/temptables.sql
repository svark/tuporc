CREATE TABLE IF NOT EXISTS PresentList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_PresentList_id ON PresentList(id);
DELETE FROM PresentList;

DROP TABLE IF EXISTS LinkPresentList;
CREATE TABLE LinkPresentList
(
    from_id INTEGER NOT NULL,
    to_id   INTEGER NOT NULL,
    srcid   INTEGER NOT NULL,
    PRIMARY KEY (from_id, to_id, srcid)
);

DROP TABLE IF EXISTS SuccessList;
Create Table SuccessList
(
    id INTEGER PRIMARY KEY not NULL
);

Drop TABLE IF EXISTS TupfileEntities;
CREATE TABLE TupfileEntities
(
    id INTEGER PRIMARY KEY not NULL,
    type INTEGER
);

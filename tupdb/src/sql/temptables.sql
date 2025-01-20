DROP TABLE IF EXISTS PresentList;
CREATE TEMP TABLE PresentList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_PresentList_id ON PresentList(id);

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

DROP TABLE IF EXISTS PresentList;
CREATE TABLE PresentList
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
CREATE TEMP VIEW ModifyList AS
    SELECT id, type from ChangeList where is_delete = 0;
CREATE TEMP VIEW DeleteList AS
    SELECT id, type from ChangeList where is_delete = 1;

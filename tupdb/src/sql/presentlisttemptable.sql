DROP TABLE IF EXISTS PresentList;
CREATE TEMP TABLE PresentList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
CREATE INDEX  idx_PresentList_id ON PresentList(id);

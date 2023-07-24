DROP TABLE IF EXISTS PresentList;
CREATE TABLE PresentList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
Create Table IF NOT EXISTS ModifyList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);
Create Table IF NOT EXISTS DeleteList
(
    id   INTEGER PRIMARY KEY not NULL,
    type INTEGER
);

Create Table IF NOT EXISTS FailedList
(
    id INTEGER PRIMARY KEY not NULL
);


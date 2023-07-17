--- A table that tracks the dynamic IO based dependencies and dependents of a rule
---  typ: 0 - dependencies read by a rule, 1 - dependents written by a rule
DROP TABLE IF EXISTS DYNIO;
CREATE TABLE DYNIO
(
    path VARCHAR(4096) not NULL,
    pid  INTEGER       not NULL  --- process id of the rule that created this entry
    ,
    gen  INTEGER       not NULL, --- generation number of the process that created this entry. This is used to detect stale entries
    typ  INTEGER       not NULL  --- 0 - dependencies read by a rule, 1 - dependents written by a rule
);
CREATE INDEX DYNIOIDX ON DYNIO (pid);

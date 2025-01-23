
DROP TABLE IF EXISTS TUPPATHBUF;
CREATE TABLE TUPPATHBUF AS
SELECT node.id as id, node.dir as dir, node.mtime_ns as mtime_ns, DIRPATHBUF.name || '/' || node.name as name
from Node
         inner join DIRPATHBUF ON
    (NODE.dir = DIRPATHBUF.id and node.type = (SELECT N.type_index from NodeType N where N.type = 'TupF'));
CREATE INDEX TupPATHBUFIdIndex On TUPPATHBUF (id);


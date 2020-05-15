curl -s -X GET http://localhost:8083/connectors/jdbc-nested-set-sink/tasks/0/status | jq




## Incremented

-- without timestamp
CREATE TABLE treeinc(id integer primary key autoincrement not null, name varchar(255), lft integer, rgt integer);

INSERT INTO treeinc (name, lft, rgt) VALUES ('A', 1, 6);
INSERT INTO treeinc (name, lft, rgt) VALUES ('B', 2, 3);
INSERT INTO treeinc (name, lft, rgt) VALUES ('C', 4, 5);

INSERT INTO treeinc (name, lft, rgt) VALUES ('D', 6, 7);

curl -s -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @jdbc-nested-set-inc-source.json | jq


curl -s -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @jdbc-nested-set-inc-sink.json | jq




## Timestamp incremented


-- with timestamp
-- sqlite doesn't work with timestamp 
CREATE TABLE treetstmpinc(id integer primary key autoincrement not null, name varchar(255), lft integer, rgt integer, modified DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP);

--postgres 
CREATE TABLE treetstmpinc(id bigserial, name varchar(255), lft integer, rgt integer, modified TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT timezone('utc' :: TEXT, now()), primary key (id));


INSERT INTO treetstmpinc (name, lft, rgt) VALUES ('A', 1, 6);
INSERT INTO treetstmpinc (name, lft, rgt) VALUES ('B', 2, 3);
INSERT INTO treetstmpinc (name, lft, rgt) VALUES ('C', 4, 5);


curl -s -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @jdbc-nested-set-tstmp-inc-source.json | jq


curl -s -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @jdbc-nested-set-tstmp-inc-sink.json | jq

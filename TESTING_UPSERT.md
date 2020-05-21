Testing instructions
====================


## Database setup
Setup a Postgres database based on Docker

```
docker run --name nested-set-postgres -p 5432:5432 -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=db -d postgres:12
```

If the database is already setup

```
docker start nested-set-postgres
```

Create the tables required for testing:

```
CREATE TABLE nested_set_node_source(
                                id bigserial,
                                label varchar(256),
                                lft int NOT NULL,
                                rgt int NOT NULL,
                                active boolean NOT NULL,
                                created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT timezone('utc' :: TEXT, now()),
                                updated TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT timezone('utc' :: TEXT, now()),
                                primary key (id)
);



CREATE TABLE nested_set_node_sink(
                                id bigint,
                                label varchar(256),
                                lft int NOT NULL,
                                rgt int NOT NULL,
                                active boolean NOT NULL,
                                created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT timezone('utc' :: TEXT, now()),
                                updated TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT timezone('utc' :: TEXT, now()),
                                primary key (id)
);


CREATE TABLE nested_set_node_sink_log(
                                    log_id bigserial,
                                    operation_type int NOT NULL,
                                    id bigint,
                                    label varchar(256),
                                    lft int NOT NULL,
                                    rgt int NOT NULL,
                                    active boolean NOT NULL,
                                    created TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT timezone('utc' :: TEXT, now()),
                                    updated TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT timezone('utc' :: TEXT, now()),
                                    primary key (log_id)
);

CREATE TABLE nested_set_node_log_offset(
                                           log_table_name varchar(256),
                                           log_table_offset bigint,
                                           primary key (log_table_name)
);

INSERT INTO nested_set_node_log_offset (log_table_name, log_table_offset) VALUES ('nested_set_node_sink_log', 0);
```

In case that the tables existed already, just truncate their content:

```
TRUNCATE TABLE nested_set_node_source;
TRUNCATE TABLE nested_set_node_sink;
TRUNCATE TABLE nested_set_node_sink_log;
TRUNCATE TABLE nested_set_node_log_offset;

INSERT INTO nested_set_node_log_offset (log_table_name, log_table_offset) VALUES ('nested_set_node_sink_log', 0);
```


## Setup the connectors

### Start confluent ecosystem

```
confluent local start
```

Make sure that Kafka Connect is up & running

```
curl http://localhost:8083/connectors
```

### Setup kafka-connect-jdbc source connector

```
curl -s -X POST -H "Accept:application/json" \
     -H  "Content-Type:application/json" \
     http://localhost:8083/connectors/ \
     --data '{
               "name": "jdbc-nested-set-node-source",
               "config": {
                 "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                 "tasks.max": "1",
                 "connection.url": "jdbc:postgresql://localhost:5432/postgres",
                 "connection.user": "postgres",
                 "connection.password": "postgres",
                 "mode":"timestamp+incrementing",
                 "timestamp.column.name":"updated",
                 "incrementing.column.name": "id",
                 "table.whitelist":"nested_set_node_source",
                 "topic.prefix": "findinpath.",
                 "name": "jdbc-nested-set-node-source"
               }
             }
     ' | jq
```

### kafka-connect-nested-set-jdbc-sink connector

```
curl -s -X POST -H "Accept:application/json" \
     -H  "Content-Type:application/json" \
     http://localhost:8083/connectors/ \
     --data '{
               "name": "jdbc-nested-set-node-sink",
               "config": {
                    "name": "jdbc-nested-set-node-sink",
                    "connector.class": "com.findinpath.connect.nestedset.jdbc.NestedSetJdbcSinkConnector",
                    "tasks.max": "1",
                    "topics": "findinpath.nested_set_node_source",
                    "connection.url": "jdbc:postgresql://localhost:5432/postgres",
                    "connection.user": "postgres",
                    "connection.password": "postgres",
                    "pk.fields": "id",
                    "table.name": "nested_set_node_sink",
                    "table.left.column.name": "lft",
                    "table.rgt.column.name": "rgt",
                    "log.table.name": "nested_set_node_sink_log",
                    "log.table.primary.key.column.name": "log_id",
                    "log.table.operation.type.column.name": "operation_type",
                    "log.offset.table.name": "nested_set_node_log_offset",
                    "log.offset.table.log.table.column.name": "log_table_name",
                    "log.offset.table.offset.column.name": "log_table_offset"
               }
             }
     ' | jq
```


## Sync data from `nested_set_node_source` towards `nested_set_node_sink` table


Insert initial nested set model
```sql
INSERT INTO nested_set_node_source (label, lft, rgt, active) VALUES ('A', 1, 6, true);
INSERT INTO nested_set_node_source (label, lft, rgt, active) VALUES ('B', 2, 3, true);
INSERT INTO nested_set_node_source (label, lft, rgt, active) VALUES ('C', 4, 5, true);
```

Verify whether there is content in the kafka-connect-jdbc topic

```
./kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic findinpath.nested_set_node_source  --property print.key=true --property schema.registry.url=http://localhost:8081 --from-beginning
```


Update nested set model

```sql
INSERT INTO nested_set_node_source (label, lft, rgt, active) VALUES ('D', 6, 7, true);
UPDATE nested_set_node_source set rgt=8, updated=timezone('utc' :: TEXT, now()) where label='A';
```


The nested set model entries should be synced between the `nested_set_node_source` and `nested_set_node_sink`
tables:

```
select * from nested_set_node_source;

select * from nested_set_node_sink;
select * from nested_set_node_sink_log;
``` 
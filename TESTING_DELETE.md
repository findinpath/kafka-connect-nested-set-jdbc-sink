Testing instructions
====================

## Install Postgres debezium connector

Debezium setup:

https://docs.confluent.io/current/connect/debezium-connect-postgres/index.html

```
confluent-hub install debezium/debezium-connector-postgresql:latest
```

## Database setup

Setup a Postgres database based on Docker

```
docker pull debezium/example-postgres
docker run -it --rm --name nested-set-postgres-debezium -p 5432:5432 \
-e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=postgres \
debezium/example-postgres

```


Create the tables required for testing:

```
CREATE TABLE nested_set_node_source(
                                id bigserial,
                                label varchar(256),
                                lft int NOT NULL,
                                rgt int NOT NULL,
                                active boolean NOT NULL,
                                primary key (id)
);



CREATE TABLE nested_set_node_sink(
                                id bigint,
                                label varchar(256),
                                lft int NOT NULL,
                                rgt int NOT NULL,
                                active boolean NOT NULL,
                                primary key (id)
);


CREATE TABLE nested_set_node_sink_log(
                                    log_id bigserial,
                                    operation_type int NOT NULL,
                                    id bigint,
                                    label varchar(256),
                                    lft int,
                                    rgt int,
                                    active boolean,
                                    primary key (log_id)
);

CREATE TABLE nested_set_node_log_offset(
                                           log_table_name varchar(256),
                                           log_table_offset bigint,
                                           primary key (log_table_name)
);

INSERT INTO nested_set_node_log_offset (log_table_name, log_table_offset) VALUES ('nested_set_node_sink_log', 0);
```

**NOTE** that in the DDL for the table `nested_set_node_sink_log` there 
are specified no `NOT NULL` constraints for the `lft`, `rgt` and `operation_type` fields.

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

### Setup debezium-connect-postgres connector

```
curl -s -X POST -H "Accept:application/json" \
     -H  "Content-Type:application/json" \
     http://localhost:8083/connectors/ \
     --data '{
               "name": "jdbc-nested-set-node-source-debezium",
               "config": {
                   "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                   "tasks.max": "1",
                   "database.hostname": "0.0.0.0",
                   "database.port": "5432",
                   "database.user": "postgres",
                   "database.password": "postgres",
                   "database.dbname" : "postgres",
                   "database.server.name": "dbserver1",
                   "schema.whitelist": "public"
                   }
              }
     ' | jq
```

Check the exported contents of the table `nested_set_node_source`:
```
kafka-avro-console-consumer --bootstrap-server localhost:9092 \
  --topic dbserver1.public.nested_set_node_source \
  --property print.key=true \
  --property schema.registry.url=http://localhost:8081 \
  --from-beginning | jq
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
                    "topics": "dbserver1.public.nested_set_node_source",
                    "connection.url": "jdbc:postgresql://localhost:5432/postgres",
                    "connection.user": "postgres",
                    "connection.password": "postgres",
                    "transforms": "unwrap",                                                  
                    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                    "transforms.unwrap.drop.tombstones":"false",
                    "transforms.unwrap.delete.handling.mode":"none", 
                    "pk.mode": "record_key",
                    "delete.enabled": true,
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

**NOTE** that in the sink connector's configuration there is a Debezium transformer used
for coping with Debezium deletions:

```
"transforms": "unwrap",                                                  
"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
"transforms.unwrap.drop.tombstones":"false",
"transforms.unwrap.delete.handling.mode":"none", 
```

More information of how to setup the `ExtractNewRecordState` extraction can be found at

https://debezium.io/documentation/reference/configuration/event-flattening.html


## Sync data from `nested_set_node_source` towards `nested_set_node_sink` table


Insert initial nested set model
```sql
INSERT INTO nested_set_node_source (label, lft, rgt, active) VALUES ('A', 1, 6, true);
INSERT INTO nested_set_node_source (label, lft, rgt, active) VALUES ('B', 2, 3, true);
INSERT INTO nested_set_node_source (label, lft, rgt, active) VALUES ('C', 4, 5, true);
```


Perform deletion on the nested set model

```sql
DELETE FROM nested_set_node_source where label='C';
UPDATE nested_set_node_source set rgt=4 where label='A';
```


The nested set model entries should be synced between the `nested_set_node_source` and `nested_set_node_sink`
tables:

```
select * from nested_set_node_source;

select * from nested_set_node_sink;
select * from nested_set_node_sink_log;
``` 
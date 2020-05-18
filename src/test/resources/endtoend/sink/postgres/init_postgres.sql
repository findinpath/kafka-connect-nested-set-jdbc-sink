CREATE TABLE nested_set_node(
    id bigint,
    label varchar(256),
    lft int NOT NULL,
    rgt int NOT NULL,
    active boolean NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    primary key (id)
);


CREATE TABLE nested_set_node_log(
    log_id bigserial,
    id bigint,
    label varchar(256),
    lft int NOT NULL,
    rgt int NOT NULL,
    active boolean NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    primary key (id)
);

CREATE TABLE nested_set_node_log_offset(
    log_table_name varchar(256),
    log_table_offset bigint,
    primary key (log_table_name)
);

INSERT INTO nested_set_node_log_offset (log_table_name, log_table_offset) VALUES ('nested_set_node_log', 0);
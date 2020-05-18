CREATE TABLE nested_set_node(
    id bigserial,
    label varchar(256),
    lft int NOT NULL,
    rgt int NOT NULL,
    active boolean NOT NULL,
    created TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    updated TIMESTAMP WITHOUT TIME ZONE DEFAULT timezone('utc' :: TEXT, now()),
    primary key (id)
);
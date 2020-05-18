create table nested_set_sync_log_offset(
    log_table_name VARCHAR(256),
    log_table_offset bigint not null,
    primary key (log_table_name)
);
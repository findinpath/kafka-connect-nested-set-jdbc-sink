create table nested_set_sync_log_offset(
    log_table_name VARCHAR2(256) not null,
    log_table_offset NUMBER(19,0) not null,
    primary key (log_table_name)
);
create table nested_set_sync_log_offset(
    log_table_name text not null constraint nested_set_sync_log_offset_pkey primary key,
    log_table_offset integer not null
);
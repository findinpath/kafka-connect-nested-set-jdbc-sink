package com.findinpath.connect.nestedset.jdbc.sink.metadata;

import java.sql.ResultSetMetaData;
import java.util.List;

public class ResultSetRecords {
    private final ResultSetMetaData resultSetMetaData;
    private final List<List<Object>> records;

    public ResultSetRecords(ResultSetMetaData resultSetMetaData, List<List<Object>> records) {
        this.resultSetMetaData = resultSetMetaData;
        this.records = records;
    }

    public ResultSetMetaData getResultSetMetaData() {
        return resultSetMetaData;
    }

    public List<List<Object>> getRecords() {
        return records;
    }
}

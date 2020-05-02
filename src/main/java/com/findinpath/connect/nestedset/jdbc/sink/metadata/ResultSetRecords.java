package com.findinpath.connect.nestedset.jdbc.sink.metadata;

import java.util.List;

public class ResultSetRecords {
    private final List<String> columnNames;
    private final List<List<Object>> records;

    public ResultSetRecords(List<String> columnNames, List<List<Object>> records) {
        this.columnNames = columnNames;
        this.records = records;
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<Object>> getRecords() {
        return records;
    }
}

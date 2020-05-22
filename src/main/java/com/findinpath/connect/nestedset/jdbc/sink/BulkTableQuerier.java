package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.ResultSetRecords;
import com.findinpath.connect.nestedset.jdbc.util.ExpressionBuilder;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class BulkTableQuerier {
    private final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);

    private final DatabaseDialect dialect;
    private final TableId tableId;


    public BulkTableQuerier(DatabaseDialect dialect,
                            TableId tableId) {
        this.dialect = dialect;
        this.tableId = tableId;
    }

    public ResultSetRecords extractRecords(Connection connection) throws SQLException {
        try (PreparedStatement stmt = createPreparedStatement(connection); ResultSet resultSet = executeQuery(stmt)) {

            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
            List<String> columnNames = new ArrayList<>();
            for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++)
                columnNames.add(resultSetMetaData.getColumnName(i));

            List<List<Object>> columnValuesList = new ArrayList<>();
            while (resultSet.next()) {
                List<Object> columnValues = new ArrayList<>();
                for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
                    columnValues.add(resultSet.getObject(i));
                }
                columnValuesList.add(columnValues);
            }


            return new ResultSetRecords(columnNames, columnValuesList);
        }
    }

    private ResultSet executeQuery(PreparedStatement stmt) throws SQLException {
        log.trace("Statement to execute: {}", stmt.toString());
        return stmt.executeQuery();
    }

    private PreparedStatement createPreparedStatement(Connection db) throws SQLException {
        ExpressionBuilder builder = dialect.expressionBuilder();
        builder.append("SELECT * FROM ").append(tableId);

        String query = builder.toString();
        log.info("Begin using SQL query: {}", query);

        return dialect.createPreparedStatement(db, query);
    }

}

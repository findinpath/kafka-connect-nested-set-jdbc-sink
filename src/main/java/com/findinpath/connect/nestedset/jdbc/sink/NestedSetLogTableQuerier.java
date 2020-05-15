package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.ResultSetRecords;
import com.findinpath.connect.nestedset.jdbc.util.ColumnId;
import com.findinpath.connect.nestedset.jdbc.util.ExpressionBuilder;
import com.findinpath.connect.nestedset.jdbc.util.QuoteMethod;
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

public class NestedSetLogTableQuerier {
    private final Logger log = LoggerFactory.getLogger(NestedSetLogTableQuerier.class);

    private final DatabaseDialect dialect;
    private final TableId logTableId;
    private final ColumnId logTableIncrementingColumn;
    private final TableId logOffsetTableId;
    private final ColumnId logOffsetTableLogTableColumn;
    private final ColumnId logOffsetTableOffsetColumn;


    public NestedSetLogTableQuerier(DatabaseDialect dialect,
                                    TableId logTableId,
                                    ColumnId logTableIncrementingColumn,
                                    TableId logOffsetTableId,
                                    ColumnId logOffsetTableLogTableColumn,
                                    ColumnId logOffsetTableOffsetColumn
    ) {
        this.dialect = dialect;
        this.logTableId = logTableId;
        this.logTableIncrementingColumn = logTableIncrementingColumn;
        this.logOffsetTableId = logOffsetTableId;
        this.logOffsetTableLogTableColumn = logOffsetTableLogTableColumn;
        this.logOffsetTableOffsetColumn = logOffsetTableOffsetColumn;
    }

    public ResultSetRecords extractRecordsForSynchronization(Connection connection) throws SQLException {
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
        String logTableName = ExpressionBuilder.create()
                .setQuoteIdentifiers(QuoteMethod.NEVER)
                .append(logTableId)
                .toString();

        stmt.setString(1, logTableName);
        log.trace("Statement to execute: {}", stmt.toString());
        return stmt.executeQuery();
    }

    private PreparedStatement createPreparedStatement(Connection db) throws SQLException {
        ExpressionBuilder builder = dialect.expressionBuilder();
        builder.append("SELECT * FROM ").append(logTableId);
        dialect.appendWhereCriteria(builder, logTableIncrementingColumn,
                logOffsetTableId, logOffsetTableLogTableColumn, logOffsetTableOffsetColumn);

        String query = builder.toString();
        log.debug("Using SQL query: {}", query);

        return dialect.createPreparedStatement(db, query);
    }

}

package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.ResultSetRecords;
import com.findinpath.connect.nestedset.jdbc.sink.tree.NestedSetNode;
import com.findinpath.connect.nestedset.jdbc.sink.tree.TreeBuilder;
import com.findinpath.connect.nestedset.jdbc.sink.tree.TreeNode;
import com.findinpath.connect.nestedset.jdbc.util.ColumnId;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class NestedSetSynchronizer {
    private static final Logger log = LoggerFactory
            .getLogger(NestedSetSynchronizer.class);

    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final TableId tableId;
    private final String tablePrimaryKeyColumnName;
    private final String tableLeftColumnName;
    private final String tableRightColumnName;
    private final TableId logTableId;
    private final String logTablePrimaryKeyColumnName;
    private final TableId logOffsetTableId;
    private final String logOffsetTableLogTableColumnName;
    private final String logOffsetTableOffsetColumnName;
    private final NestedSetLogTableQuerier nestedSetLogTableQuerier;
    private final BulkTableQuerier nestedSetTableQuerier;


    public NestedSetSynchronizer(
            JdbcSinkConfig config,
            DatabaseDialect dbDialect,
            DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;

        this.tableId = dbDialect.parseTableIdentifier(config.tableName);
        this.tablePrimaryKeyColumnName = config.tablePrimaryKeyColumnName;
        this.tableLeftColumnName = config.tableLeftColumnName;
        this.tableRightColumnName = config.tableRightColumnName;
        this.logTableId = dbDialect.parseTableIdentifier(config.logTableName);
        this.logTablePrimaryKeyColumnName = config.logTablePrimaryKeyColumnName;
        this.logOffsetTableId = dbDialect.parseTableIdentifier(config.logOffsetTableName);
        this.logOffsetTableLogTableColumnName = config.logOffsetTableLogTableColumnName;
        this.logOffsetTableOffsetColumnName = config.logOffsetTableOffsetColumnName;

        nestedSetLogTableQuerier = new NestedSetLogTableQuerier(dbDialect, logTableId,
                new ColumnId(logTableId, logTablePrimaryKeyColumnName),
                logOffsetTableId,
                new ColumnId(logOffsetTableId, config.logOffsetTableLogTableColumnName),
                new ColumnId(logOffsetTableId, config.logOffsetTableOffsetColumnName));

        nestedSetTableQuerier = new BulkTableQuerier(dbDialect, tableId);
    }

    public void synchronize(Connection connection) throws SQLException {
        dbStructure.createLogOffsetTableIfNecessary(
                config,
                connection,
                logOffsetTableId);

        // get nested set log entries
        ResultSetRecords nestedSetLogTableUpdates = nestedSetLogTableQuerier.extractRecordsForSynchronization(connection);
        List<List<Object>>  nestedSetLogTableRecords = nestedSetLogTableUpdates.getRecords();
        log.info("Outstanding nested set log entries in the table {} to be synchronized {}", logTableId, nestedSetLogTableRecords.size());
        if (nestedSetLogTableRecords == null || nestedSetLogTableRecords.isEmpty()){
            return;
        }

        // deduplicate nestedSetLogTableUpdates
        int logTablePrimaryKeyColumnIndex = getColIdxByName(logTablePrimaryKeyColumnName, nestedSetLogTableUpdates.getColumnNames())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + logTablePrimaryKeyColumnName));
        int logTableNestedSetNodeIdColumnIndex = getColIdxByName(tablePrimaryKeyColumnName, nestedSetLogTableUpdates.getColumnNames())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + tablePrimaryKeyColumnName));
        int logTableNestedSetNodeLeftColumnIndex = getColIdxByName(tableLeftColumnName, nestedSetLogTableUpdates.getColumnNames())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + tableLeftColumnName));
        int logTableNestedSetNodeRightColumnIndex = getColIdxByName(tableRightColumnName, nestedSetLogTableUpdates.getColumnNames())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + tableRightColumnName));

        Function<List<Object>, Long> getLogTableRecordId = recordValues -> getColumnAsLong(recordValues, logTablePrimaryKeyColumnIndex);
        Function<List<Object>, Long> getLogTableRecordNestedSetNodeId = recordValues -> getColumnAsLong(recordValues, logTableNestedSetNodeIdColumnIndex);
        Function<List<Object>, Integer> getLogTableRecordNestedSetNodeLeft = recordValues -> getColumnValueAsInteger(recordValues, logTableNestedSetNodeLeftColumnIndex);
        Function<List<Object>, Integer> getLogTableRecordNestedSetNodeRight = recordValues -> getColumnValueAsInteger(recordValues, logTableNestedSetNodeRightColumnIndex);

        List<List<Object>> deduplicatedNestedSetLogTableRecords = deduplicateNestedSetLogTableRecords(nestedSetLogTableRecords,
                getLogTableRecordId, getLogTableRecordNestedSetNodeId);
        log.info("Outstanding deduplicated nested set log entries in the table {} to be synchronized {}", logTableId, deduplicatedNestedSetLogTableRecords.size());

        if (!isValidNestedSetNodeContent(deduplicatedNestedSetLogTableRecords, logTableId,
                getLogTableRecordId, getLogTableRecordNestedSetNodeLeft, getLogTableRecordNestedSetNodeRight)) {
            return;
        }

        // get nested set entries
        ResultSetRecords nestedSetTableRecords = nestedSetTableQuerier.extractRecords(connection);
        int tablePrimaryKeyColumnIndex = getColIdxByName(tablePrimaryKeyColumnName, nestedSetTableRecords.getColumnNames())
                .orElseThrow(() -> new SQLException("The table " + tableId + " doesn't contain the expected column " + tablePrimaryKeyColumnName));
        int tableLeftColumnIndex = getColIdxByName(tableLeftColumnName, nestedSetTableRecords.getColumnNames())
                .orElseThrow(() -> new SQLException("The table " + tableId + " doesn't contain the expected column " + tableLeftColumnName));
        int tableRightColumnIndex = getColIdxByName(tableRightColumnName, nestedSetTableRecords.getColumnNames())
                .orElseThrow(() -> new SQLException("The table " + tableId + " doesn't contain the expected column " + tableRightColumnName));
        Function<List<Object>, Long> getTableRecordId = recordValues -> getColumnAsLong(recordValues, tablePrimaryKeyColumnIndex);
        Function<List<Object>, Integer> getTableRecordLeft = recordValues -> getColumnValueAsInteger(recordValues, tableLeftColumnIndex);
        Function<List<Object>, Integer> getTableRecordRight = recordValues -> getColumnValueAsInteger(recordValues, tableRightColumnIndex);

        if (!isValidNestedSetNodeContent(nestedSetTableRecords.getRecords(), tableId,
                getTableRecordId, getTableRecordLeft, getTableRecordRight)) {
            return;
        }

        // if OK
        if (isValidNestedSetModel(nestedSetTableRecords.getRecords(),
                getTableRecordId, getTableRecordLeft, getTableRecordRight,
                deduplicatedNestedSetLogTableRecords,
                getLogTableRecordNestedSetNodeId, getLogTableRecordNestedSetNodeLeft, getLogTableRecordNestedSetNodeRight)) {

            Map<Long, List<Object>> id2NestedSetRecordMap = nestedSetTableRecords.getRecords().stream()
                    .collect(Collectors.toMap(getTableRecordId, Function.identity()));
            Predicate<List<Object>> isNestedSetNodeAlreadyPersisted = (List<Object> nestedSetLogRecord) ->
                    id2NestedSetRecordMap.containsKey(getLogTableRecordNestedSetNodeId.apply(nestedSetLogRecord));
            Map<Boolean, List<List<Object>>> nestedSetLogRecordsToSynchronizePartitions = deduplicatedNestedSetLogTableRecords.stream()
                    .collect(Collectors.partitioningBy(isNestedSetNodeAlreadyPersisted));
            List<List<Object>> newNestedSetRecordsSortedByLogId = nestedSetLogRecordsToSynchronizePartitions.get(false);
            List<List<Object>> updatedNestedSetRecordsSortedByLogId = nestedSetLogRecordsToSynchronizePartitions.get(true);

            long latestNestedSetLogTableRecordId = getLogTableRecordId.apply(deduplicatedNestedSetLogTableRecords.get(deduplicatedNestedSetLogTableRecords.size() - 1));

            applyUpdates(connection,
                    nestedSetLogTableUpdates.getColumnNames(),
                    logTablePrimaryKeyColumnIndex,
                    logTableNestedSetNodeIdColumnIndex,
                    newNestedSetRecordsSortedByLogId,
                    updatedNestedSetRecordsSortedByLogId,
                    latestNestedSetLogTableRecordId
            );
        }
    }

    private List<List<Object>> deduplicateNestedSetLogTableRecords(List<List<Object>> nestedSetLogTableRecordsValues,
                                                                   Function<List<Object>, Long> getLogTableRecordId,
                                                                   Function<List<Object>, Long> getLogTableRecordNestedSetNodeId) {
        Map<Long, List<Object>> nestedSetNodeId2LatestLogTableRecordValues = new HashMap<>();
        for (List<Object> recordValues : nestedSetLogTableRecordsValues) {
            Long nestedSetNodeId = getLogTableRecordNestedSetNodeId.apply(recordValues);
            List<Object> latestLogTableRecordValues = nestedSetNodeId2LatestLogTableRecordValues.get(nestedSetNodeId);
            if (latestLogTableRecordValues == null ||
                    (getLogTableRecordId.apply(recordValues) > getLogTableRecordId.apply(latestLogTableRecordValues))) {
                nestedSetNodeId2LatestLogTableRecordValues.put(nestedSetNodeId, recordValues);
            }
        }
        return new ArrayList<>(nestedSetNodeId2LatestLogTableRecordValues.values());
    }

    private boolean isValidNestedSetNodeContent(List<List<Object>> recordsValues,
                                                TableId tableId,
                                                Function<List<Object>, Long> getTableRecordId,
                                                Function<List<Object>, Integer> getLogTableRecordNestedSetNodeLeft,
                                                Function<List<Object>, Integer> getLogTableRecordNestedSetNodeRight) {
        boolean invalidNestedSetTableRecordsFound = false;
        for (List<Object> recordValues : recordsValues) {
            Long id = getTableRecordId.apply(recordValues);
            if (!isValidNestedSetNode(recordValues, getLogTableRecordNestedSetNodeLeft, getLogTableRecordNestedSetNodeRight)) {
                invalidNestedSetTableRecordsFound = true;
                log.error("The entry with the ID {} of the table {} contains invalid nested set coordinates", tableId, id);
            }
        }
        return !invalidNestedSetTableRecordsFound;
    }

    private boolean isValidNestedSetModel(List<List<Object>> nestedSetTableRecordsValues,
                                          Function<List<Object>, Long> getNestedSetTableRecordId,
                                          Function<List<Object>, Integer> getNestedSetTableRecordLeft,
                                          Function<List<Object>, Integer> getNestedSetTableRecordRight,
                                          List<List<Object>> nestedSetLogTableRecordsValues,
                                          Function<List<Object>, Long> getLogTableRecordNestedSetNodeId,
                                          Function<List<Object>, Integer> getLogTableRecordNestedSetNodeLeft,
                                          Function<List<Object>, Integer> getLogTableRecordNestedSetNodeRight) {
        // merge the updates from the nested set log table into the existing nested set
        List<NestedSetNode> updatedNestedSetNodes = getUpdatedNestedSet(nestedSetTableRecordsValues,
                getNestedSetTableRecordId, getNestedSetTableRecordLeft, getNestedSetTableRecordRight,
                nestedSetLogTableRecordsValues,
                getLogTableRecordNestedSetNodeId, getLogTableRecordNestedSetNodeLeft, getLogTableRecordNestedSetNodeRight);

        Optional<TreeNode> rootNode = TreeBuilder.buildTree(updatedNestedSetNodes);

        return rootNode.isPresent();
    }

    private void applyUpdates(Connection connection,
                              List<String> logTableColumnNames,
                              int logTablePrimaryKeyColumnIndex,
                              int logTableNestedSetNodeIdColumnIndex,
                              List<List<Object>> newNestedSetLogTableRecordsValues,
                              List<List<Object>> updatedNestedSetLogTableRecordsValues,
                              long latestNestedSetLogTableRecordId
    ) throws SQLException {
        log.info("Applying nested set table updates to the table {} with contents from the table {}", tableId, logTableId);

        //    save nested set log offset
        upsertLogOffset(connection, latestNestedSetLogTableRecordId);

        //    insert new entries in the nested set table
        insertIntoNestedSetTable(connection,
                logTableColumnNames,
                logTablePrimaryKeyColumnIndex,
                newNestedSetLogTableRecordsValues);

        //    update existing entries in the nested set table
        updatedNestedSetTable(connection,
                logTableColumnNames,
                logTablePrimaryKeyColumnIndex,
                logTableNestedSetNodeIdColumnIndex,
                updatedNestedSetLogTableRecordsValues);
    }

    private void upsertLogOffset(Connection connection,
                                 long latestNestedSetLogTableRecordId) throws SQLException {
        String sql = dbDialect.buildUpsertQueryStatement(logOffsetTableId,
                Collections.singletonList(new ColumnId(logOffsetTableId, logOffsetTableLogTableColumnName)),
                Collections.singletonList(new ColumnId(logOffsetTableId, logOffsetTableOffsetColumnName)));

        log.debug("Updating log offset  table ID: {} to {}", logOffsetTableId, latestNestedSetLogTableRecordId);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, logTableId.toString());
            stmt.setLong(2, latestNestedSetLogTableRecordId);

            stmt.execute();
        }
    }

    private void insertIntoNestedSetTable(Connection connection,
                                          List<String> columnNames,
                                          int logTablePrimaryKeyColumnIndex,
                                          List<List<Object>> nestedSetLogTableRecordsValues) throws SQLException {

        List<ColumnId> columns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            if (logTablePrimaryKeyColumnIndex != i) {
                columns.add(new ColumnId(tableId, columnName));
            }
        }

        String sql = dbDialect.buildInsertStatement(tableId, Collections.emptyList(), columns);

        log.debug("Using INSERT SQL query: {}", sql);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (List<Object> nestedSetLogTableRecordValues : nestedSetLogTableRecordsValues) {
                int parameterIndex = 1;
                for (int i = 0; i < columnNames.size(); i++) {
                    if (logTablePrimaryKeyColumnIndex != i) {
                        stmt.setObject(parameterIndex++, nestedSetLogTableRecordValues.get(i));
                    }
                }
                stmt.addBatch();
            }

            stmt.executeBatch();
        }
    }

    private void updatedNestedSetTable(Connection connection,
                                       List<String> columnNames,
                                       int logTablePrimaryKeyColumnIndex,
                                       int logTableNestedSetNodeIdColumnIndex,
                                       List<List<Object>> nestedSetLogTableRecordsValues) throws SQLException {

        ColumnId keyColumn = new ColumnId(tableId, tablePrimaryKeyColumnName);
        List<ColumnId> nonKeyColumns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++) {
            String columnName = columnNames.get(i);
            if (i != logTablePrimaryKeyColumnIndex && i != logTableNestedSetNodeIdColumnIndex) {
                nonKeyColumns.add(new ColumnId(tableId, columnName));
            }
        }
        String sql = dbDialect.buildUpdateStatement(tableId, Collections.singletonList(keyColumn), nonKeyColumns);
        log.debug("Using UPDATE SQL query: {}", sql);

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            for (List<Object> nestedSetLogTableRecordValues : nestedSetLogTableRecordsValues) {
                int parameterIndex = 1;
                for (int i = 0; i < columnNames.size(); i++) {
                    if (i != logTablePrimaryKeyColumnIndex && i != logTableNestedSetNodeIdColumnIndex) {
                        stmt.setObject(parameterIndex++, nestedSetLogTableRecordValues.get(i));
                    }
                }
                stmt.setObject(parameterIndex,
                        nestedSetLogTableRecordValues.get(logTableNestedSetNodeIdColumnIndex));

                stmt.addBatch();
            }

            stmt.executeBatch();
        }
    }


    private static List<NestedSetNode> getUpdatedNestedSet(
            List<List<Object>> nestedSetTableRecordsValues,
            Function<List<Object>, Long> getNestedSetTableRecordId,
            Function<List<Object>, Integer> getNestedSetTableRecordLeft,
            Function<List<Object>, Integer> getNestedSetTableRecordRight,
            List<List<Object>> nestedSetLogTableRecordsValues,
            Function<List<Object>, Long> getLogTableRecordNestedSetNodeId,
            Function<List<Object>, Integer> getLogTableRecordNestedSetNodeLeft,
            Function<List<Object>, Integer> getLogTableRecordNestedSetNodeRight) {
        Map<Long, NestedSetNode> nestedSetNodeId2NestedSetNodeMap = nestedSetTableRecordsValues.stream()
                .collect(Collectors.toMap(getNestedSetTableRecordId, recordValues -> new NestedSetNode() {
                    @Override
                    public int getLeft() {
                        return getNestedSetTableRecordLeft.apply(recordValues);
                    }

                    @Override
                    public int getRight() {
                        return getNestedSetTableRecordRight.apply(recordValues);
                    }

                }));

        // apply updates
        for (List<Object> nestedSetLogTableRecordValues : nestedSetLogTableRecordsValues) {
            nestedSetNodeId2NestedSetNodeMap.put(getLogTableRecordNestedSetNodeId.apply(nestedSetLogTableRecordValues),
                    new NestedSetNode() {
                        @Override
                        public int getLeft() {
                            return getLogTableRecordNestedSetNodeLeft.apply(nestedSetLogTableRecordValues);
                        }

                        @Override
                        public int getRight() {
                            return getLogTableRecordNestedSetNodeRight.apply(nestedSetLogTableRecordValues);
                        }
                    });
        }

        return new ArrayList<>(nestedSetNodeId2NestedSetNodeMap.values());
    }

    private static boolean isValidNestedSetNode(List<Object> nestedSetRecordValues,
                                                Function<List<Object>, Integer> getNestedSetRecordLeftValue,
                                                Function<List<Object>, Integer> getNestedSetRecordRightValue
    ) {
        Integer left = getNestedSetRecordLeftValue.apply(nestedSetRecordValues);
        Integer right = getNestedSetRecordRightValue.apply(nestedSetRecordValues);

        return left != null && right != null && left < right;
    }

    private static Long getColumnAsLong(List<Object> recordValues, int index) {
        Number number = ((Number) recordValues.get(index));
        return number == null ? null : number.longValue();
    }

    private static Integer getColumnValueAsInteger(List<Object> recordValues, int index) {
        Number number = ((Number) recordValues.get(index));
        return number == null ? null : number.intValue();
    }

    /**
     * Returns the column number of the column with the given name in the
     * result set metadata columns.
     *
     * @param name a <code>String</code> object that is the name of a column in
     *             the result set metadata columns.
     */
    private static Optional<Integer> getColIdxByName(String name, List<String> columnNames) throws SQLException {

        for (int i = 0; i < columnNames.size(); ++i) {
            String colName = columnNames.get(i);
            if (colName != null)
                if (name.equalsIgnoreCase(colName))
                    return Optional.of(i);
        }
        return Optional.empty();
    }
}

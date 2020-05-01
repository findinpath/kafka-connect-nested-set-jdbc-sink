package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.ResultSetRecords;
import com.findinpath.connect.nestedset.jdbc.util.CachedConnectionProvider;
import com.findinpath.connect.nestedset.jdbc.util.ColumnId;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class NestedSetSynchronizer {
    private static final Logger log = LoggerFactory
            .getLogger(NestedSetSynchronizer.class);

    private final CachedConnectionProvider cachedConnectionProvider;

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


        this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
            @Override
            protected void onConnect(Connection connection) throws SQLException {
                log.info("NestedSetSynchronizer Connected");
                connection.setAutoCommit(false);
            }
        };

        nestedSetLogTableQuerier = new NestedSetLogTableQuerier(dbDialect, logTableId,
                new ColumnId(logTableId, logTablePrimaryKeyColumnName),
                logOffsetTableId,
                new ColumnId(logOffsetTableId, config.logOffsetTableLogTableColumnName),
                new ColumnId(logOffsetTableId, config.logOffsetTableOffsetColumnName));

        nestedSetTableQuerier = new BulkTableQuerier(dbDialect, tableId);
    }

    public void synchronize() throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();

        dbStructure.createLogOffsetTableIfNecessary(
                config,
                connection,
                logOffsetTableId);

        // get nested set log entries
        ResultSetRecords nestedSetLogTableUpdates = nestedSetLogTableQuerier.extractUnsynchronizedRecords(connection);

        // deduplicate nestedSetLogTableUpdates
        int logTablePrimaryKeyColumnIndex = getColIdxByName(logTablePrimaryKeyColumnName, nestedSetLogTableUpdates.getResultSetMetaData())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + logTablePrimaryKeyColumnName));
        int logTableNestedSetNodeIdColumnIndex = getColIdxByName(tablePrimaryKeyColumnName, nestedSetLogTableUpdates.getResultSetMetaData())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + tablePrimaryKeyColumnName));
        int logTableNestedSetNodeLeftColumnIndex = getColIdxByName(tableLeftColumnName, nestedSetLogTableUpdates.getResultSetMetaData())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + tableLeftColumnName));
        int logTableNestedSetNodeRightColumnIndex = getColIdxByName(tableRightColumnName, nestedSetLogTableUpdates.getResultSetMetaData())
                .orElseThrow(() -> new SQLException("The table " + logTableId + " doesn't contain the expected column " + tableRightColumnName));

        Function<List<Object>, Long> getLogTableRecordId = recordValues -> getColumnAsLong(recordValues, logTablePrimaryKeyColumnIndex);
        Function<List<Object>, Long> getLogTableRecordNestedSetNodeId = recordValues -> getColumnAsLong(recordValues, logTableNestedSetNodeIdColumnIndex);
        Function<List<Object>, Integer> getLogTableRecordNestedSetNodeLeft = recordValues -> getColumnValueAsInteger(recordValues, logTableNestedSetNodeLeftColumnIndex);
        Function<List<Object>, Integer> getLogTableRecordNestedSetNodeRight = recordValues -> getColumnValueAsInteger(recordValues, logTableNestedSetNodeRightColumnIndex);

        BinaryOperator<List<Object>> takeNestedSetLogRecordWithTheMaxId = (nestedSetLogRecord1, nestedSetLogRecord2) ->
                getLogTableRecordId.apply(nestedSetLogRecord1) > getLogTableRecordId.apply(nestedSetLogRecord2) ? nestedSetLogRecord1 : nestedSetLogRecord2;

        List<List<Object>> deduplicatedNestedSetLogTableRecords = nestedSetLogTableUpdates.getRecords()
                .stream()
                .collect(Collectors.groupingBy(getLogTableRecordNestedSetNodeId))
                .values()
                .stream()
                .map(nestedSetLogRecordsWithTheSameNestedSetNodeId -> nestedSetLogRecordsWithTheSameNestedSetNodeId.stream().reduce(takeNestedSetLogRecordWithTheMaxId))
                .map(Optional::get)
                .sorted(Comparator.comparing(getLogTableRecordId))
                .collect(Collectors.toList());

        long latestNestedSetLogTableRecordId = getLogTableRecordId.apply(deduplicatedNestedSetLogTableRecords.get(deduplicatedNestedSetLogTableRecords.size() - 1));


        // get nested set entries
        ResultSetRecords nestedSetTableRecords = nestedSetTableQuerier.extractRecords(connection);
        int tablePrimaryKeyColumnIndex = getColIdxByName(tablePrimaryKeyColumnName, nestedSetTableRecords.getResultSetMetaData())
                .orElseThrow(() -> new SQLException("The table " + tableId + " doesn't contain the expected column " + tablePrimaryKeyColumnName));
        int tableLeftColumnIndex = getColIdxByName(tableLeftColumnName, nestedSetTableRecords.getResultSetMetaData())
                .orElseThrow(() -> new SQLException("The table " + tableId + " doesn't contain the expected column " + tableLeftColumnName));
        int tableRightColumnIndex = getColIdxByName(tableRightColumnName, nestedSetTableRecords.getResultSetMetaData())
                .orElseThrow(() -> new SQLException("The table " + tableId + " doesn't contain the expected column " + tableRightColumnName));
        Function<List<Object>, Long> getTableRecordId = recordValues -> getColumnAsLong(recordValues, tablePrimaryKeyColumnIndex);
        Function<List<Object>, Integer> getTableRecordLeft = recordValues -> getColumnValueAsInteger(recordValues, tablePrimaryKeyColumnIndex);
        Function<List<Object>, Integer> getTableRecordRight = recordValues -> getColumnValueAsInteger(recordValues, tablePrimaryKeyColumnIndex);

        Map<Long, List<Object>> id2NestedSetRecordMap = nestedSetTableRecords.getRecords().stream()
                .collect(Collectors.toMap(getTableRecordId, Function.identity()));
        Predicate<List<Object>> isNestedSetNodeAlreadyPersisted = (List<Object> nestedSetLogRecord) ->
                id2NestedSetRecordMap.containsKey(getLogTableRecordNestedSetNodeId.apply(nestedSetLogRecord));
        Map<Boolean, List<List<Object>>> unsynchronizedNestedSetLogRecordsPartitions = deduplicatedNestedSetLogTableRecords.stream()
                .collect(Collectors.partitioningBy(isNestedSetNodeAlreadyPersisted));
        List<List<Object>> newNestedSetRecordsSortedByLogId = unsynchronizedNestedSetLogRecordsPartitions.get(false);
        List<List<Object>> updatedNestedSetRecordsSortedByLogId = unsynchronizedNestedSetLogRecordsPartitions.get(true);


        // try to merge
        List<NestedSetNode> updatedNestedSet = getUpdatedNestedSet(nestedSetTableRecords.getRecords(), getTableRecordId, getTableRecordLeft, getTableRecordRight,
                deduplicatedNestedSetLogTableRecords,getLogTableRecordNestedSetNodeId, getLogTableRecordNestedSetNodeLeft, getLogTableRecordNestedSetNodeRight);

        // if OK
        //    save nested set log offset
        //    insert new entries in the nested set table
        //    update existing entries in the nested set table

        connection.commit();
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
        Map<Long, NestedSetNode> id2NestedSetRecordMap = nestedSetTableRecordsValues.stream()
                .collect(Collectors.toMap(getNestedSetTableRecordId, recordValues -> new NestedSetNode() {
                    @Override
                    public Integer getLeft() {
                        return getNestedSetTableRecordLeft.apply(recordValues);
                    }

                    @Override
                    public Integer getRight() {
                        return getNestedSetTableRecordRight.apply(recordValues);
                    }

                }));

        // apply updates
        for (List<Object> nestedSetLogTableRecordValues : nestedSetLogTableRecordsValues) {
            id2NestedSetRecordMap.put(getLogTableRecordNestedSetNodeId.apply(nestedSetLogTableRecordValues),
                    new NestedSetNode() {
                        @Override
                        public Integer getLeft() {
                            return getLogTableRecordNestedSetNodeLeft.apply(nestedSetLogTableRecordValues);
                        }

                        @Override
                        public Integer getRight() {
                            return getLogTableRecordNestedSetNodeRight.apply(nestedSetLogTableRecordValues);
                        }
                    });
        }

        return new ArrayList<>(id2NestedSetRecordMap.values());
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
     * <code>ResultSetMetaData</code> object.
     *
     * @param name a <code>String</code> object that is the name of a column in
     *             this <code>ResultSetMetaData</code> object
     */
    private static Optional<Integer> getColIdxByName(String name, ResultSetMetaData resultSetMetaData) throws SQLException {

        for (int i = 1; i <= resultSetMetaData.getColumnCount(); ++i) {
            String colName = resultSetMetaData.getColumnName(i);
            if (colName != null)
                if (name.equalsIgnoreCase(colName))
                    return Optional.of(i);
                else
                    continue;
        }
        return Optional.empty();
    }
}

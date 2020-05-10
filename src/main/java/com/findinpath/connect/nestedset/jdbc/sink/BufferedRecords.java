/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect.LogTableStatementBinder;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.FieldsMetadata;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.SchemaPair;
import com.findinpath.connect.nestedset.jdbc.util.ColumnId;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

public class BufferedRecords {
  private static final Logger log = LoggerFactory
			.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final TableId logTableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private Schema keySchema;
  private Schema valueSchema;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement updatePreparedStatement;
  private PreparedStatement deletePreparedStatement;
  private LogTableStatementBinder updateLogTableStatementBinder;
  private LogTableStatementBinder deleteLogTableStatementBinder;

  public BufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      TableId logTableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.logTableId = logTableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final List<SinkRecord> flushed = new ArrayList<>();

    boolean schemaChanged = false;
    if (!Objects.equals(keySchema, record.keySchema())) {
      keySchema = record.keySchema();
      schemaChanged = true;
    }
    if (!isNull(record.valueSchema()) && !Objects.equals(valueSchema, record.valueSchema())) {
      // value schema is not null and has changed. This is a real schema change.
      valueSchema = record.valueSchema();
      schemaChanged = true;
    }
    if (schemaChanged) {
      // Each batch needs to have the same schemas, so get the buffered records out
      flushed.addAll(flush());

      // re-initialize everything that depends on the record schema
      final SchemaPair schemaPair = new SchemaPair(
          record.keySchema(),
          record.valueSchema()
      );
      fieldsMetadata = FieldsMetadata.extract(
          tableId.tableName(),
          config.pkMode,
          config.pkFields,
          config.fieldsWhitelist,
          schemaPair
      );
      boolean dbStructureAltered = dbStructure.createOrAmendIfNecessary(
          config,
          connection,
          tableId,
          logTableId,
          fieldsMetadata
      );
      if (dbStructureAltered) {
        log.info("Db structure on the tables {} and {} was altered", tableId, logTableId);
      }

      final String insertSql = getInsertSql();
      final String deleteSql = getDeleteSql();
      log.debug(
          "sql: {} deleteSql: {} meta: {}",
          insertSql,
          deleteSql,
          fieldsMetadata
      );
      close();
      updatePreparedStatement = dbDialect.createPreparedStatement(connection, insertSql);
      updateLogTableStatementBinder = dbDialect.statementBinder(
          updatePreparedStatement,
          config.pkMode,
          schemaPair,
          fieldsMetadata
      );
      if (config.deleteEnabled && nonNull(deleteSql)) {
        deletePreparedStatement = dbDialect.createPreparedStatement(connection, deleteSql);
        deleteLogTableStatementBinder = dbDialect.statementBinder(
            deletePreparedStatement,
            config.pkMode,
            schemaPair,
            fieldsMetadata
        );
      }
    }

    records.add(record);

    if (records.size() >= config.batchSize) {
      flushed.addAll(flush());
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      log.debug("Records is empty");
      return new ArrayList<>();
    }
    log.debug("Flushing {} buffered records", records.size());
    for (SinkRecord record : records) {
      if (isNull(record.value()) && nonNull(deleteLogTableStatementBinder)) {
        deleteLogTableStatementBinder.bindRecord(record, OperationType.DELETE);
      } else {
        updateLogTableStatementBinder.bindRecord(record, OperationType.UPSERT);
      }
    }
    Optional<Long> totalUpdateCount = executeUpdates();
    long totalDeleteCount = executeDeletes();

    final long expectedCount = updateRecordCount();
    log.trace("records:{} resulting in totalUpdateCount:{} totalDeleteCount:{}",
        records.size(), totalUpdateCount, totalDeleteCount
    );
    if (totalUpdateCount.filter(total -> total != expectedCount).isPresent()) {
      throw new ConnectException(String.format(
          "Update count (%d) did not sum up to total number of records inserted (%d)",
          totalUpdateCount.get(),
          expectedCount
      ));
    }
    if (!totalUpdateCount.isPresent()) {
      log.info(
          "records:{} , but no count of the number of rows it affected is available",
          records.size()
      );
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    return flushedRecords;
  }

  /**
   * @return an optional count of all updated rows or an empty optional if no info is available
   */
  private Optional<Long> executeUpdates() throws SQLException {
    Optional<Long> count = Optional.empty();
    for (int updateCount : updatePreparedStatement.executeBatch()) {
      if (updateCount != Statement.SUCCESS_NO_INFO) {
        count = count.isPresent()
            ? count.map(total -> total + updateCount)
            : Optional.of((long) updateCount);
      }
    }
    return count;
  }

  private long executeDeletes() throws SQLException {
    long totalDeleteCount = 0;
    if (nonNull(deletePreparedStatement)) {
      for (int updateCount : deletePreparedStatement.executeBatch()) {
        if (updateCount != Statement.SUCCESS_NO_INFO) {
          totalDeleteCount += updateCount;
        }
      }
    }
    return totalDeleteCount;
  }

  private long updateRecordCount() {
    return records
        .stream()
        // ignore deletes
        .filter(record -> nonNull(record.value()) || !config.deleteEnabled)
        .count();
  }

  public void close() throws SQLException {
    log.debug(
        "Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}",
        updatePreparedStatement,
        deletePreparedStatement
    );
    if (nonNull(updatePreparedStatement)) {
      updatePreparedStatement.close();
      updatePreparedStatement = null;
    }
    if (nonNull(deletePreparedStatement)) {
      deletePreparedStatement.close();
      deletePreparedStatement = null;
    }
  }

  private String getInsertSql() {
    Set<String> nonKeyFieldNames = fieldsMetadata.nonKeyFieldNames;
    List<String> allLogTableNonKeyFieldNames = new ArrayList<>();
    allLogTableNonKeyFieldNames.add(config.logTableOperationTypeColumnName);
    allLogTableNonKeyFieldNames.addAll(nonKeyFieldNames);


    return dbDialect.buildInsertStatement(
        logTableId,
        asColumns(fieldsMetadata.keyFieldNames),
        asColumns(allLogTableNonKeyFieldNames)
    );
  }

  private String getDeleteSql() {
    return dbDialect.buildInsertStatement(
            logTableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(Arrays.asList(config.logTableOperationTypeColumnName))
    );
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
        .map(name -> new ColumnId(logTableId, name))
        .collect(Collectors.toList());
  }
}

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
import com.findinpath.connect.nestedset.jdbc.sink.metadata.FieldsMetadata;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.SinkRecordField;
import com.findinpath.connect.nestedset.jdbc.util.TableDefinition;
import com.findinpath.connect.nestedset.jdbc.util.TableDefinitions;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import com.findinpath.connect.nestedset.jdbc.util.TableType;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

public class DbStructure {
    private static final Logger log = LoggerFactory
            .getLogger(DbStructure.class);

    private final DatabaseDialect dbDialect;
    private final TableDefinitions tableDefns;

    public DbStructure(DatabaseDialect dbDialect) {
        this.dbDialect = dbDialect;
        this.tableDefns = new TableDefinitions(dbDialect);
    }


    /**
     * Creates or amends the specified tables
     *
     * @param config the configuration of the connector
     * @param connection connection to the database
     * @param tableId the nested set model table
     * @param logTableId nested set model log table
     * @param fieldsMetadata the metadata of the nested set record
     * @return whether a DDL operation was performed
     * @throws SQLException if a DDL operation was deemed necessary but failed
     */
    public boolean createOrAmendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final TableId logTableId,
            final FieldsMetadata fieldsMetadata
    ) throws SQLException {
        if (tableDefns.get(connection, logTableId) == null) {
            // Table does not yet exist, so attempt to create it ...
            createLogTable(config, connection, logTableId, fieldsMetadata.allFields.values());
        }
        if (tableDefns.get(connection, tableId) == null) {
            // Table does not yet exist, so attempt to create it ...
            createTable(config, connection, tableId, fieldsMetadata.allFields.values());
        }

        boolean tableAmended = amendIfNecessary(config, connection, tableId, fieldsMetadata, dbDialect::buildAlterTable, config.maxRetries);
        boolean logTableAmended = amendIfNecessary(config, connection, logTableId, fieldsMetadata, dbDialect::buildAlterLogTable, config.maxRetries);
        return tableAmended || logTableAmended;
    }

    public void createLogOffsetTableIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection
    ) throws SQLException {
        TableId logOffsetTableId = dbDialect.parseTableIdentifier(config.logOffsetTableName);
        if (tableDefns.get(connection, logOffsetTableId) == null) {
            // Table does not yet exist, so attempt to create it ...
            if (!config.autoCreate) {
                throw new ConnectException(
                        String.format("Table %s is missing and auto-creation is disabled", logOffsetTableId)
                );
            }

            try {
                String sql = dbDialect.buildCreateLogOffsetTableStatement(logOffsetTableId,
                        config.logOffsetTableLogTableColumnName,
                        config.logOffsetTableOffsetColumnName);
                log.info("Creating table with sql: {}", sql);
                dbDialect.applyDdlStatements(connection, Collections.singletonList(sql));
            } catch (SQLException sqle) {
                log.warn("Create failed, will attempt amend if table already exists", sqle);
                try {
                    TableDefinition newDefn = tableDefns.refresh(connection, logOffsetTableId);
                    if (newDefn == null) {
                        throw sqle;
                    }
                } catch (SQLException e) {
                    throw sqle;
                }
            }
        }
    }


    /**
     * @throws SQLException if CREATE failed
     */
    void createTable(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final Collection<SinkRecordField> fields
    ) throws SQLException {
        if (!config.autoCreate) {
            throw new ConnectException(
                    String.format("Table %s is missing and auto-creation is disabled", tableId)
            );
        }
        try {
            String sql = dbDialect.buildCreateTableStatement(tableId, fields);
            log.info("Creating table with sql: {}", sql);
            dbDialect.applyDdlStatements(connection, Collections.singletonList(sql));
        } catch (SQLException sqle) {
            log.warn("Create failed, will attempt amend if table already exists", sqle);
            try {
                TableDefinition newDefn = tableDefns.refresh(connection, tableId);
                if (newDefn == null) {
                    throw sqle;
                }
            } catch (SQLException e) {
                throw sqle;
            }
        }
    }

    void createLogTable(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final Collection<SinkRecordField> fields
    ) throws SQLException {
        if (!config.autoCreate) {
            throw new ConnectException(
                    String.format("Table %s is missing and auto-creation is disabled", tableId)
            );
        }
        try {
            List<String> sql = dbDialect.buildCreateLogTableStatements(tableId,
                    config.logTablePrimaryKeyColumnName,
                    config.logTableOperationTypeColumnName,
                    fields);
            log.info("Creating table with sql: {}", sql);
            dbDialect.applyDdlStatements(connection, sql);
        } catch (SQLException sqle) {
            log.warn("Create failed for the table {}, will attempt amend if table already exists", tableId, sqle);
            try {
                TableDefinition newDefn = tableDefns.refresh(connection, tableId);
                if (newDefn == null) {
                    throw sqle;
                }
            } catch (SQLException e) {
                throw sqle;
            }
        }
    }

    /**
     * @return whether an ALTER was successfully performed
     * @throws SQLException if ALTER was deemed necessary but failed
     */
    boolean amendIfNecessary(
            final JdbcSinkConfig config,
            final Connection connection,
            final TableId tableId,
            final FieldsMetadata fieldsMetadata,
            BiFunction<TableId, Set<SinkRecordField>, List<String>> alterTableStatementGenerator,
            final int maxRetries
    ) throws SQLException {
        // NOTE:
        //   The table might have extra columns defined (hopefully with default values), which is not
        //   a case we check for here.
        //   We also don't check if the data types for columns that do line-up are compatible.

        final TableDefinition tableDefn = tableDefns.get(connection, tableId);

        // FIXME: SQLite JDBC driver seems to not always return the PK column names?
        //    if (!tableMetadata.getPrimaryKeyColumnNames().equals(fieldsMetadata.keyFieldNames)) {
        //      throw new ConnectException(String.format(
        //          "Table %s has different primary key columns - database (%s), desired (%s)",
        //          tableName, tableMetadata.getPrimaryKeyColumnNames(), fieldsMetadata.keyFieldNames
        //      ));
        //    }

        final Set<SinkRecordField> missingFields = missingFields(
                fieldsMetadata.allFields.values(),
                tableDefn.columnNames()
        );

        if (missingFields.isEmpty()) {
            return false;
        }

        // At this point there are missing fields
        TableType type = tableDefn.type();
        switch (type) {
            case TABLE:
                // Rather than embed the logic and change lots of lines, just break out
                break;
            case VIEW:
            default:
                throw new ConnectException(
                        String.format(
                                "%s %s is missing fields (%s) and ALTER %s is unsupported",
                                type.capitalized(),
                                tableId,
                                missingFields,
                                type.jdbcName()
                        )
                );
        }

        for (SinkRecordField missingField : missingFields) {
            if (!missingField.isOptional() && missingField.defaultValue() == null) {
                throw new ConnectException(String.format(
                        "Cannot ALTER %s %s to add missing field %s, as the field is not optional and does "
                                + "not have a default value",
                        type.jdbcName(),
                        tableId,
                        missingField
                ));
            }
        }

        if (!config.autoEvolve) {
            throw new ConnectException(String.format(
                    "%s %s is missing fields (%s) and auto-evolution is disabled",
                    type.capitalized(),
                    tableId,
                    missingFields
            ));
        }

        final List<String> amendTableQueries = alterTableStatementGenerator.apply(tableId, missingFields);
        log.info(
                "Amending {} to add missing fields:{} maxRetries:{} with SQL: {}",
                tableId,
                missingFields,
                maxRetries,
                amendTableQueries
        );
        try {
            dbDialect.applyDdlStatements(connection, amendTableQueries);
        } catch (SQLException sqle) {
            if (maxRetries <= 0) {
                throw new ConnectException(
                        String.format(
                                "Failed to amend %s '%s' to add missing fields: %s",
                                type,
                                tableId,
                                missingFields
                        ),
                        sqle
                );
            }
            log.warn("Amend failed, re-attempting", sqle);
            tableDefns.refresh(connection, tableId);
            // Perhaps there was a race with other tasks to add the columns
            return amendIfNecessary(
                    config,
                    connection,
                    tableId,
                    fieldsMetadata,
                    alterTableStatementGenerator,
                    maxRetries - 1
            );
        }

        tableDefns.refresh(connection, tableId);
        return true;
    }

    Set<SinkRecordField> missingFields(
            Collection<SinkRecordField> fields,
            Set<String> dbColumnNames
    ) {
        final Set<SinkRecordField> missingFields = new HashSet<>();
        for (SinkRecordField field : fields) {
            if (!dbColumnNames.contains(field.name())) {
                log.debug("Found missing field: {}", field);
                missingFields.add(field);
            }
        }

        if (missingFields.isEmpty()) {
            return missingFields;
        }

        // check if the missing fields can be located by ignoring case
        Set<String> columnNamesLowerCase = new HashSet<>();
        for (String columnName : dbColumnNames) {
            columnNamesLowerCase.add(columnName.toLowerCase());
        }

        if (columnNamesLowerCase.size() != dbColumnNames.size()) {
            log.warn(
                    "Table has column names that differ only by case. Original columns={}",
                    dbColumnNames
            );
        }

        final Set<SinkRecordField> missingFieldsIgnoreCase = new HashSet<>();
        for (SinkRecordField missing : missingFields) {
            if (!columnNamesLowerCase.contains(missing.name().toLowerCase())) {
                missingFieldsIgnoreCase.add(missing);
            }
        }

        if (missingFieldsIgnoreCase.size() > 0) {
            log.info(
                    "Unable to find fields {} among column names {}",
                    missingFieldsIgnoreCase,
                    dbColumnNames
            );
        }

        return missingFieldsIgnoreCase;
    }
}

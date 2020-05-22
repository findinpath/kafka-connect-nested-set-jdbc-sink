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

package com.findinpath.connect.nestedset.jdbc.dialect;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialectProvider.SubprotocolBasedProvider;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.SinkRecordField;
import com.findinpath.connect.nestedset.jdbc.util.ColumnId;
import com.findinpath.connect.nestedset.jdbc.util.ExpressionBuilder;
import com.findinpath.connect.nestedset.jdbc.util.IdentifierRules;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A {@link DatabaseDialect} for SQLite.
 */
public class SqliteDatabaseDialect extends GenericDatabaseDialect {

    /**
     * Create a new dialect instance with the given connector configuration.
     *
     * @param config the connector configuration; may not be null
     */
    public SqliteDatabaseDialect(AbstractConfig config) {
        super(config, new IdentifierRules(".", "`", "`"));
    }

    @Override
    protected boolean includeTable(TableId table) {
        // SQLite JDBC driver does not correctly mark these as system tables
        return !table.tableName().startsWith("sqlite_");
    }

    @Override
    protected String getSqlType(SinkRecordField field) {
        if (field.schemaName() != null) {
            switch (field.schemaName()) {
                case Decimal.LOGICAL_NAME:
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                case Timestamp.LOGICAL_NAME:
                    return "NUMERIC";
                default:
                    // pass through to normal types
            }
        }
        switch (field.schemaType()) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return "INTEGER";
            case FLOAT32:
            case FLOAT64:
                return "REAL";
            case STRING:
                return "TEXT";
            case BYTES:
                return "BLOB";
            default:
                return super.getSqlType(field);
        }
    }

    @Override
    public List<String> buildCreateLogTableStatements(
            TableId table,
            String primaryKeyColumnName,
            String operationTypeColumnName,
            Collection<SinkRecordField> fields
    ) {
        ExpressionBuilder builder = expressionBuilder();

        builder.append("CREATE TABLE ");
        builder.append(table);
        builder.append(" (");
        builder.appendColumnName(primaryKeyColumnName);
        builder.append(" ");
        builder.append("INTEGER PRIMARY KEY AUTOINCREMENT,");
        builder.appendColumnName(operationTypeColumnName);
        builder.append(" ");
        builder.append("INTEGER NOT NULL,");
        writeNullableColumnsSpec(builder, fields);
        builder.append(")");
        return Arrays.asList(builder.toString());
    }

    @Override
    public List<String> buildAlterTable(
            TableId table,
            Collection<SinkRecordField> fields
    ) {
        final List<String> queries = new ArrayList<>(fields.size());
        for (SinkRecordField field : fields) {
            queries.addAll(super.buildAlterTable(table, Collections.singleton(field)));
        }
        return queries;
    }

    @Override
    public List<String> buildAlterLogTable(
            TableId table,
            Collection<SinkRecordField> fields
    ) {
        final List<String> queries = new ArrayList<>(fields.size());
        for (SinkRecordField field : fields) {
            queries.addAll(super.buildAlterLogTable(table, Collections.singleton(field)));
        }
        return queries;
    }

    @Override
    public String buildUpsertQueryStatement(
            TableId table,
            Collection<ColumnId> keyColumns,
            Collection<ColumnId> nonKeyColumns
    ) {
        ExpressionBuilder builder = expressionBuilder();
        builder.append("INSERT OR REPLACE INTO ");
        builder.append(table);
        builder.append("(");
        builder.appendList()
                .delimitedBy(",")
                .transformedBy(ExpressionBuilder.columnNames())
                .of(keyColumns, nonKeyColumns);
        builder.append(") VALUES(");
        builder.appendMultiple(",", "?", keyColumns.size() + nonKeyColumns.size());
        builder.append(")");
        return builder.toString();
    }

    @Override
    protected String currentTimestampDatabaseQuery() {
        return "SELECT strftime('%Y-%m-%d %H:%M:%S.%f','now')";
    }

    /**
     * The provider for {@link SqliteDatabaseDialect}.
     */
    public static class Provider extends SubprotocolBasedProvider {
        public Provider() {
            super(SqliteDatabaseDialect.class.getSimpleName(), "sqlite");
        }

        @Override
        public DatabaseDialect create(AbstractConfig config) {
            return new SqliteDatabaseDialect(config);
        }
    }
}

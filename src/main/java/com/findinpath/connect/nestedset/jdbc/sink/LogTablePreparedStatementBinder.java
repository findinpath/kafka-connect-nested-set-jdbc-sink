/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect.LogTableStatementBinder;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.FieldsMetadata;
import com.findinpath.connect.nestedset.jdbc.sink.metadata.SchemaPair;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.Objects.isNull;

public class LogTablePreparedStatementBinder implements LogTableStatementBinder {

  private final JdbcSinkConfig.PrimaryKeyMode pkMode;
  private final PreparedStatement statement;
  private final SchemaPair schemaPair;
  private final FieldsMetadata fieldsMetadata;
  private final DatabaseDialect dialect;

  public LogTablePreparedStatementBinder(
      DatabaseDialect dialect,
      PreparedStatement statement,
      JdbcSinkConfig.PrimaryKeyMode pkMode,
      SchemaPair schemaPair,
      FieldsMetadata fieldsMetadata
  ) {
    this.dialect = dialect;
    this.pkMode = pkMode;
    this.statement = statement;
    this.schemaPair = schemaPair;
    this.fieldsMetadata = fieldsMetadata;
  }

  public void bindRecord(SinkRecord record, OperationType operationType) throws SQLException {
    final Struct valueStruct = (Struct) record.value();
    final boolean isDelete = isNull(valueStruct);
    // Assumption: the relevant SQL has placeholders for keyFieldNames first followed by
    //             nonKeyFieldNames, in iteration order for all INSERT queries
    //             the relevant SQL has placeholders for keyFieldNames,
    //             in iteration order for all DELETE queries
    //             the relevant SQL has placeholders for nonKeyFieldNames first followed by
    //             keyFieldNames, in iteration order for all UPDATE queries

    int index = 1;
    index = bindKeyFields(record, index);
    bindField(index++, Schema.INT32_SCHEMA, operationType.ordinal());
    if (!isDelete) {
      bindNonKeyFields(record, valueStruct, index);
    }
    statement.addBatch();
  }

  protected int bindKeyFields(SinkRecord record, int index) throws SQLException {
    switch (pkMode) {
      case RECORD_KEY: {
        if (schemaPair.keySchema.type().isPrimitive()) {
          assert fieldsMetadata.keyFieldNames.size() == 1;
          bindField(index++, schemaPair.keySchema, record.key());
        } else {
          for (String fieldName : fieldsMetadata.keyFieldNames) {
            final Field field = schemaPair.keySchema.field(fieldName);
            bindField(index++, field.schema(), ((Struct) record.key()).get(field));
          }
        }
      }
      break;

      case RECORD_VALUE: {
        for (String fieldName : fieldsMetadata.keyFieldNames) {
          final Field field = schemaPair.valueSchema.field(fieldName);
          bindField(index++, field.schema(), ((Struct) record.value()).get(field));
        }
      }
      break;

      default:
        throw new ConnectException("Unknown primary key mode: " + pkMode);
    }
    return index;
  }

  protected int bindNonKeyFields(
      SinkRecord record,
      Struct valueStruct,
      int index
  ) throws SQLException {
    for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
      final Field field = record.valueSchema().field(fieldName);
      bindField(index++, field.schema(), valueStruct.get(field));
    }
    return index;
  }

  protected void bindField(int index, Schema schema, Object value) throws SQLException {
    dialect.bindField(statement, index, schema, value);
  }
}

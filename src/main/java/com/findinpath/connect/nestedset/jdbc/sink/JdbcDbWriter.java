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
import com.findinpath.connect.nestedset.jdbc.util.CachedConnectionProvider;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory
			.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final TableId tableId;
  private final TableId logTableId;
  final CachedConnectionProvider cachedConnectionProvider;

  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.tableId = dbDialect.parseTableIdentifier(config.tableName);
    this.logTableId = dbDialect.parseTableIdentifier(config.logTableName);

    this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
      @Override
      protected void onConnect(Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records) throws SQLException {
    final Connection connection = cachedConnectionProvider.getConnection();

    try {
      BufferedRecords buffer = new BufferedRecords(config, tableId,
              logTableId, dbDialect,
              dbStructure, connection);

      for (SinkRecord record : records) {
        buffer.add(record);
      }

      log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
      buffer.flush();

      buffer.close();

      connection.commit();
    }catch(SQLException e){
      connection.rollback();
      throw e;
    }
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }
}

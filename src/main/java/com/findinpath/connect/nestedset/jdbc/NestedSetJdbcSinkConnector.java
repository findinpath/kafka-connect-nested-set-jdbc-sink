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

package com.findinpath.connect.nestedset.jdbc;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialects;
import com.findinpath.connect.nestedset.jdbc.sink.DbStructure;
import com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig;
import com.findinpath.connect.nestedset.jdbc.sink.NestedSetJdbcSinkTask;
import com.findinpath.connect.nestedset.jdbc.util.Version;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NestedSetJdbcSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory
			.getLogger(NestedSetJdbcSinkConnector.class);

  private Map<String, String> configProps;

  public Class<? extends Task> taskClass() {
    return NestedSetJdbcSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  @Override
  public void start(Map<String, String> props) {
    configProps = props;
    createLogOffsetTableIfNecessary();
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return JdbcSinkConfig.CONFIG_DEF;
  }

  @Override
  public Config validate(Map<String, String> connectorConfigs) {
    return super.validate(connectorConfigs);
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  private void createLogOffsetTableIfNecessary() {
    JdbcSinkConfig config = new JdbcSinkConfig(configProps);
    try (DatabaseDialect dialect = getDatabaseDialect(config);
         Connection connection = dialect.getConnection()) {
      connection.setAutoCommit(false);
      final DbStructure dbStructure = new DbStructure(dialect);
      dbStructure.createLogOffsetTableIfNecessary(
              config,
              connection);

      connection.commit();
    } catch (SQLException sqle) {
      throw new ConnectException(sqle);
    }
  }




  private DatabaseDialect getDatabaseDialect(JdbcSinkConfig config){
    if (config.dialectName != null && !config.dialectName.trim().isEmpty()) {
      return DatabaseDialects.create(config.dialectName, config);
    } else {
      return DatabaseDialects.findBestFor(config.connectionUrl, config);
    }
  }
}

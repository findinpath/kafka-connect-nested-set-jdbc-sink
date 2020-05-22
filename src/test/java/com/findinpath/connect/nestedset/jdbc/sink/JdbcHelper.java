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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static com.findinpath.connect.nestedset.jdbc.util.StringUtils.isBlank;

public class JdbcHelper implements AutoCloseable {
    private static final Logger log = LoggerFactory
            .getLogger(JdbcHelper.class);
    private final Connection connection;

    public JdbcHelper(String jdbcUrl) throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl);
    }

    public JdbcHelper(String jdbcUrl, String username, String password) throws SQLException {
        if (!isBlank(username)) {
            Properties connectionProps = new Properties();
            connectionProps.put("user", username);
            connectionProps.put("password", password);
            connection = DriverManager.getConnection(jdbcUrl, connectionProps);
        } else {
            connection = DriverManager.getConnection(jdbcUrl);
        }
    }

    public int select(final String query, final JdbcHelper.ResultSetReadCallback callback) throws SQLException {
        log.info("Executing with callback SQL: {}", query);
        int count = 0;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    callback.read(rs);
                    count++;
                }
            }
        }
        return count;
    }

    public void execute(String sql) throws SQLException {
        log.info("Executing SQL: {}", sql);
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
        }
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }

    public interface ResultSetReadCallback {
        void read(final ResultSet rs) throws SQLException;
    }
}

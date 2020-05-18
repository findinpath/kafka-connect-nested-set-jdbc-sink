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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class SqliteJdbcDbWriterTest extends JdbcDbWriterTest {
    private static Path dbPath;

    private static final String CREATE_NESTED_SET_SYNC_OFFSET_LOG_DDL = "CREATE TABLE IF NOT EXISTS \"nested_set_sync_log_offset\" (\n" +
            "\"log_table_name\" TEXT NOT NULL,\n" +
            "\"log_table_offset\" INTEGER NOT NULL,\n" +
            "PRIMARY KEY(\"log_table_name\"));";

    @BeforeAll
    public static void setUpClass() throws Exception {
        dbPath = Paths.get(SqliteJdbcDbWriterTest.class.getSimpleName() + ".db");
        Files.deleteIfExists(dbPath);
        try (JdbcHelper jdbcHelper = new JdbcHelper("jdbc:sqlite:" + dbPath)) {
            jdbcHelper.execute(CREATE_NESTED_SET_SYNC_OFFSET_LOG_DDL);
        }
    }

    @AfterAll
    public static void tearDownClass() throws SQLException, IOException {
        Files.deleteIfExists(dbPath);
    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:sqlite:" + dbPath;
    }

    @Override
    protected String getJdbcUsername() {
        return null;
    }

    @Override
    protected String getJdbcPassword() {
        return null;
    }
}

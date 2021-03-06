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

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class PostgresJdbcDbWriterTest extends JdbcDbWriterTest {
    protected static final String POSTGRES_DB_NAME = "findinpath";
    protected static final String POSTGRES_DB_USERNAME = "sa";
    protected static final String POSTGRES_DB_PASSWORD = "p@ssw0rd!";

    @Container
    protected static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
            .withInitScript("sink/init_jdbcdbdriver_postgres.sql")
            .withDatabaseName(POSTGRES_DB_NAME)
            .withUsername(POSTGRES_DB_USERNAME)
            .withPassword(POSTGRES_DB_PASSWORD);

    @Override
    protected String getJdbcUrl() {
        return postgreSQLContainer.getJdbcUrl();
    }

    @Override
    protected String getJdbcUsername() {
        return POSTGRES_DB_USERNAME;
    }

    @Override
    protected String getJdbcPassword() {
        return POSTGRES_DB_PASSWORD;
    }
}

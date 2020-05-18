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

import org.testcontainers.containers.MSSQLServerContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class SqlServerJdbcDbWriterTest extends JdbcDbWriterTest {
    @Container
    protected static MSSQLServerContainer msSqlContainer = new MSSQLServerContainer<>()
            .withInitScript("sink/init_jdbcdbdriver_mssql.sql");

    @Override
    protected String getJdbcUrl() {
        return msSqlContainer.getJdbcUrl();
    }

    @Override
    protected String getJdbcUsername() {
        return msSqlContainer.getUsername();
    }

    @Override
    protected String getJdbcPassword() {
        return msSqlContainer.getPassword();
    }
}

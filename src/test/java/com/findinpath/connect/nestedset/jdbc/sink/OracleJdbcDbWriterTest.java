package com.findinpath.connect.nestedset.jdbc.sink;
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

import com.findinpath.testcontainers.OracleContainer;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;

@Disabled
@Testcontainers
public class OracleJdbcDbWriterTest extends JdbcDbWriterTest {

    @Container
    protected static OracleContainer oracleContainer = new OracleContainer("oracle/database:18.4.0-xe")
            .withInitScript("sink/init_jdbcdbdriver_oracle.sql");

    @Override
    protected String getJdbcUrl() {
        return oracleContainer.getJdbcUrl();
    }

    @Override
    protected String getJdbcUsername() {
        return oracleContainer.getUsername();
    }

    @Override
    protected String getJdbcPassword() {
        return oracleContainer.getPassword();
    }

    @Override
    protected void dropTableIfExists(String tableName) throws SQLException {
        try {
            jdbcHelper.execute("DROP TABLE " + tableName);
        }catch(SQLException e){
            // swallow eventual exceptions when the table doesn't exists
        }
    }
}

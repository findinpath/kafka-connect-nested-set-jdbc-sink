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

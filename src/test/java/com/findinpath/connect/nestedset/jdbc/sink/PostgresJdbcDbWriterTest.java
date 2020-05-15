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
            .withInitScript("sink/postgres/init_jdbcdbdriver_postgres.sql")
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

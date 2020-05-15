package com.findinpath.connect.nestedset.jdbc.sink;

import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class MysqlJdbcDbWriterTest extends JdbcDbWriterTest {
    protected static final String MYSQL_DB_NAME = "findinpath";
    protected static final String MYSQL_DB_USERNAME = "sa";
    protected static final String MYSQL_DB_PASSWORD = "p@ssw0rd!";

    @Container
    protected static MySQLContainer postgreSQLContainer = new MySQLContainer<>("mysql:8")
            .withInitScript("sink/postgres/init_jdbcdbdriver_mysql.sql")
            .withDatabaseName(MYSQL_DB_NAME)
            .withUsername(MYSQL_DB_USERNAME)
            .withPassword(MYSQL_DB_PASSWORD);

    @Override
    protected String getJdbcUrl() {
        return postgreSQLContainer.getJdbcUrl();
    }

    @Override
    protected String getJdbcUsername() {
        return MYSQL_DB_USERNAME;
    }

    @Override
    protected String getJdbcPassword() {
        return MYSQL_DB_PASSWORD;
    }
}

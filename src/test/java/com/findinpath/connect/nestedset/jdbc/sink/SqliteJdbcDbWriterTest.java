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

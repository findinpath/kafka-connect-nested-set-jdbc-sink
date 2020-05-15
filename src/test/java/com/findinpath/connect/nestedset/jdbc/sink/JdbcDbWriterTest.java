package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.AUTO_CREATE;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.AUTO_EVOLVE;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.CONNECTION_PASSWORD;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.CONNECTION_URL;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.CONNECTION_USER;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.LOG_TABLE_NAME;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.TABLE_NAME;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

@Testcontainers
public class JdbcDbWriterTest {

    protected static final String POSTGRES_DB_NAME = "findinpath";
    protected static final String POSTGRES_NETWORK_ALIAS = "postgres";
    protected static final String POSTGRES_DB_USERNAME = "sa";
    protected static final String POSTGRES_DB_PASSWORD = "p@ssw0rd!";


    protected static final String TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT = "id";
    protected static final String TABLE_LEFT_COLUMN_NAME_DEFAULT = "lft";
    protected static final String TABLE_RIGHT_COLUMN_NAME_DEFAULT = "rgt";


    @Container
    protected static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
            .withNetworkAliases(POSTGRES_NETWORK_ALIAS)
            .withInitScript("sink/postgres/init_postgres.sql")
            .withDatabaseName(POSTGRES_DB_NAME)
            .withUsername(POSTGRES_DB_USERNAME)
            .withPassword(POSTGRES_DB_PASSWORD);

    private JdbcDbWriter jdbcDbWriter;

    private JdbcHelper jdbcHelper;

    @BeforeEach
    public void setup() throws SQLException {

        Map<String, String> props = new HashMap<>();
        props.put(AUTO_CREATE, "true");
        props.put(AUTO_EVOLVE, "true");
        props.put(TABLE_NAME, "nested_set");

        props.put(LOG_TABLE_NAME, "nested_set_log");
        props.put(CONNECTION_URL, postgreSQLContainer.getJdbcUrl());
        props.put(CONNECTION_USER, POSTGRES_DB_USERNAME);
        props.put(CONNECTION_PASSWORD, POSTGRES_DB_PASSWORD);

        JdbcSinkConfig config = new JdbcSinkConfig(props);
        DatabaseDialect dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
        final DbStructure dbStructure = new DbStructure(dialect);

        jdbcDbWriter = new JdbcDbWriter(config, dialect, dbStructure);

        jdbcHelper = new JdbcHelper(postgreSQLContainer.getJdbcUrl(), POSTGRES_DB_USERNAME, POSTGRES_DB_PASSWORD);
    }

    @Test
    public void autoCreateAccuracy() throws SQLException {
        String topic = "nested-set";

        Schema keySchema = Schema.INT64_SCHEMA;

        long rootId = 1L;
        Schema valueSchema = SchemaBuilder.struct()
                .field(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, Schema.INT64_SCHEMA)
                .field(TABLE_LEFT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
                .field(TABLE_RIGHT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
                .field("label", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, rootId)
                .put(TABLE_LEFT_COLUMN_NAME_DEFAULT, 1)
                .put(TABLE_RIGHT_COLUMN_NAME_DEFAULT, 2)
                .put("label", "Food");

        jdbcDbWriter.write(Collections.singleton(new SinkRecord(topic, 0,
                keySchema, rootId,
                valueSchema, valueStruct, 0)));

        int nestedSetLogEntriesCount = jdbcHelper.select("select count(*) from nested_set_log", rs -> { });
        assertThat(nestedSetLogEntriesCount, equalTo(1));

        int nestedSetEntriesCount = jdbcHelper.select("select count(*) from nested_set", rs -> { });
        assertThat(nestedSetEntriesCount, equalTo(1));

        AtomicLong offset = new AtomicLong(0);
        jdbcHelper.select("select log_table_offset from nested_set_sync_log_offset where log_table_name='nested_set_log'", rs -> {
            offset.set(rs.getLong(1));
        });

        AtomicLong maxNestedSetLogId = new AtomicLong(0);
        jdbcHelper.select("select max(log_id) from nested_set_log", rs -> {
            maxNestedSetLogId.set(rs.getLong(1));
        });
        assertThat("The recorded offset should match the max log_id of the tuples from nested_set_log table",
                offset.get(), equalTo(maxNestedSetLogId.get()));
    }
}

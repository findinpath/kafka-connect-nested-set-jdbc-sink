package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.AUTO_CREATE;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.AUTO_EVOLVE;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.CONNECTION_PASSWORD;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.CONNECTION_URL;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.CONNECTION_USER;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.DELETE_ENABLED;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.LOG_TABLE_NAME;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.PK_FIELDS;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.PK_MODE;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.PrimaryKeyMode.RECORD_KEY;
import static com.findinpath.connect.nestedset.jdbc.sink.JdbcSinkConfig.TABLE_NAME;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public abstract class JdbcDbWriterTest {
    protected static final String NESTED_SET_TABLE_NAME = "nested_set";
    protected static final String NESTED_SET_LOG_TABLE_NAME = "nested_set_log";
    protected static final String NESTED_SET_LOG_OFFSET_TABLE_NAME = "nested_set_sync_log_offset";

    protected static final String TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT = "id";
    protected static final String TABLE_LEFT_COLUMN_NAME_DEFAULT = "lft";
    protected static final String TABLE_RIGHT_COLUMN_NAME_DEFAULT = "rgt";
    protected static final String TABLE_LABEL_COLUMN_NAME = "label";
    protected static final String TABLE_MODIFIED_COLUMN_NAME = "modified";

    protected static final String TOPIC = "nested-set";

    protected static final Schema NESTED_SET_INCREMENTED_SCHEMA = SchemaBuilder.struct()
            .field(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, Schema.INT64_SCHEMA)
            .field(TABLE_LEFT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
            .field(TABLE_RIGHT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
            .field(TABLE_LABEL_COLUMN_NAME, Schema.STRING_SCHEMA)
            .build();

    protected static final Schema NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA = SchemaBuilder.struct()
            .field(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, Schema.INT64_SCHEMA)
            .field(TABLE_LEFT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
            .field(TABLE_RIGHT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
            .field(TABLE_LABEL_COLUMN_NAME, Schema.STRING_SCHEMA)
            .field(TABLE_MODIFIED_COLUMN_NAME, Timestamp.SCHEMA)
            .build();

    private JdbcHelper jdbcHelper;


    protected abstract String getJdbcUrl();

    protected abstract String getJdbcUsername();

    protected abstract String getJdbcPassword();

    protected JdbcDbWriter createJdbcDbWriter(boolean autoCreate,
                                              boolean autoEvolve) {
        return createJdbcDbWriter(autoCreate,autoEvolve,false);
    }


    protected JdbcDbWriter createJdbcDbWriter(boolean autoCreate,
                                              boolean autoEvolve,
                                              boolean deleteEnabled) {
        Map<String, Object> props = new HashMap<>();
        props.put(AUTO_CREATE, String.valueOf(autoCreate));
        props.put(AUTO_EVOLVE, String.valueOf(autoEvolve));
        props.put(DELETE_ENABLED, String.valueOf(deleteEnabled));
        if (deleteEnabled){
            props.put(PK_MODE, String.valueOf(RECORD_KEY));
            props.put(PK_FIELDS, Arrays.asList(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT));
        }
        props.put(TABLE_NAME, NESTED_SET_TABLE_NAME);
        props.put(LOG_TABLE_NAME, NESTED_SET_LOG_TABLE_NAME);
        props.put(CONNECTION_URL, getJdbcUrl());
        props.put(CONNECTION_USER, getJdbcUsername());
        props.put(CONNECTION_PASSWORD, getJdbcPassword());

        JdbcSinkConfig config = new JdbcSinkConfig(props);
        DatabaseDialect dialect = DatabaseDialects.findBestFor(config.connectionUrl, config);
        final DbStructure dbStructure = new DbStructure(dialect);

        return new JdbcDbWriter(config, dialect, dbStructure);
    }

    @BeforeEach
    public void setup() throws SQLException {
        jdbcHelper = new JdbcHelper(getJdbcUrl(), getJdbcUsername(), getJdbcPassword());
        jdbcHelper.execute("DROP TABLE IF EXISTS " + NESTED_SET_TABLE_NAME);
        jdbcHelper.execute("DROP TABLE IF EXISTS " + NESTED_SET_LOG_TABLE_NAME);
        jdbcHelper.execute("DELETE FROM " + NESTED_SET_LOG_OFFSET_TABLE_NAME);

    }

    @Test
    public void autoCreateAccuracy() throws SQLException {
        JdbcDbWriter jdbcDbWriter = createJdbcDbWriter(true, true);
        Schema keySchema = Schema.INT64_SCHEMA;

        long rootId = 1L;
        Struct rootStruct = createNestedSetIncrementedStruct(rootId, 1, 2, "Root");

        jdbcDbWriter.write(singleton(new SinkRecord(TOPIC, 0,
                keySchema, rootId,
                NESTED_SET_INCREMENTED_SCHEMA, rootStruct, 0)));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 1);
        assertTableSize(NESTED_SET_TABLE_NAME, 1);
        assertOffsetAccuracyForSynchronizedRecords();
    }


    @Test
    public void nestedSetSynchronizationEventuallyTakesPlace() throws SQLException {
        JdbcDbWriter jdbcDbWriter = createJdbcDbWriter(true, true);

        Schema keySchema = Schema.INT64_SCHEMA;
        long rootId = 1L;
        long childId = 2L;
        Struct rootStruct = createNestedSetIncrementedStruct(rootId, 1, 4, "Root");
        Struct childStruct = createNestedSetIncrementedStruct(childId, 2, 3, "Child");

        jdbcDbWriter.write(singleton(new SinkRecord(TOPIC, 0,
                keySchema, rootId,
                NESTED_SET_INCREMENTED_SCHEMA, rootStruct, 0)));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 1);
        assertTableSize(NESTED_SET_TABLE_NAME, 0);

        jdbcDbWriter.write(singleton(new SinkRecord(TOPIC, 0,
                keySchema, childId,
                NESTED_SET_INCREMENTED_SCHEMA, childStruct, 1)));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 2);
        assertTableSize(NESTED_SET_TABLE_NAME, 2);
        assertOffsetAccuracyForSynchronizedRecords();
    }


    @Test
    public void nestedSetSynchronizationEventuallyUpdatesExistingNodes() throws SQLException {
        JdbcDbWriter jdbcDbWriter = createJdbcDbWriter(true, true);

        Schema keySchema = Schema.INT64_SCHEMA;

        long rootId = 1L;
        long childId = 2L;
        Struct rootStruct = createNestedSetTimestampIncrementedStruct(rootId, 1, 2, "Root", 1474661401000L);

        jdbcDbWriter.write(singleton(new SinkRecord(TOPIC, 0,
                keySchema, rootId,
                NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, rootStruct, 0)));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 1);
        assertTableSize(NESTED_SET_TABLE_NAME, 1);
        assertOffsetAccuracyForSynchronizedRecords();

        Struct childStruct = createNestedSetTimestampIncrementedStruct(childId, 2, 3, "Child", 1474661402000L);
        Struct updatedRootStruct = createNestedSetTimestampIncrementedStruct(rootId, 1, 4, "Root", 1474661402000L);

        jdbcDbWriter.write(asList(
                new SinkRecord(TOPIC, 0, keySchema, childId, NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, childStruct, 1),
                new SinkRecord(TOPIC, 0, keySchema, rootId, NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, updatedRootStruct, 2)
                )
        );

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 3);
        assertTableSize(NESTED_SET_TABLE_NAME, 2);
        assertOffsetAccuracyForSynchronizedRecords();
    }


    @Test
    public void autoEvolutionAccuracy() throws SQLException {
        JdbcDbWriter jdbcDbWriter = createJdbcDbWriter(true, true);

        Schema keySchema = Schema.INT64_SCHEMA;

        long rootId = 1L;
        Struct rootStruct = createNestedSetTimestampIncrementedStruct(rootId, 1, 2, "Root", 1474661401000L);

        jdbcDbWriter.write(singleton(new SinkRecord(TOPIC, 0,
                keySchema, rootId,
                NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, rootStruct, 0)));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 1);
        assertTableSize(NESTED_SET_TABLE_NAME, 1);
        assertOffsetAccuracyForSynchronizedRecords();

        Schema newValueSchema = SchemaBuilder.struct()
                .field(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, Schema.INT64_SCHEMA)
                .field(TABLE_LEFT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
                .field(TABLE_RIGHT_COLUMN_NAME_DEFAULT, Schema.INT32_SCHEMA)
                .field(TABLE_LABEL_COLUMN_NAME, Schema.STRING_SCHEMA)
                .field(TABLE_MODIFIED_COLUMN_NAME, Timestamp.SCHEMA)
                .field("active", Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .build();

        Struct updatedRootStruct = new Struct(newValueSchema)
                .put(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, 1L)
                .put(TABLE_LEFT_COLUMN_NAME_DEFAULT, 1)
                .put(TABLE_RIGHT_COLUMN_NAME_DEFAULT, 2)
                .put(TABLE_LABEL_COLUMN_NAME, "Root")
                .put(TABLE_MODIFIED_COLUMN_NAME, new Date(1474661402000L))
                .put("active", true);

        jdbcDbWriter.write(singleton(new SinkRecord(TOPIC, 0, keySchema, rootId, newValueSchema, updatedRootStruct, 2)));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 2);
        assertTableSize(NESTED_SET_TABLE_NAME, 1);
        assertOffsetAccuracyForSynchronizedRecords();

        assertThat(
                jdbcHelper.select("select active from " + NESTED_SET_TABLE_NAME + " where " + TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT + " = " + rootId,
                        rs -> {
                            assertThat(rs.getBoolean(1), equalTo(true));
                        }),
                equalTo(1));
    }

    @Test
    public void deletionAccuracy() throws SQLException {
        JdbcDbWriter jdbcDbWriter = createJdbcDbWriter(true, true, true);

        Schema keySchema = Schema.INT64_SCHEMA;
        long rootId = 1L;
        long childId = 2L;
        Struct rootStruct = createNestedSetTimestampIncrementedStruct(rootId, 1, 4, "Root", 1474661401000L);
        Struct childStruct = createNestedSetTimestampIncrementedStruct(childId, 2, 3, "Child", 1474661401000L);

        jdbcDbWriter.write(asList(
                new SinkRecord(TOPIC, 0, keySchema, rootId, NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, rootStruct, 0),
                new SinkRecord(TOPIC, 0, keySchema, childId, NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, childStruct, 1)
        ));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 2);
        assertTableSize(NESTED_SET_TABLE_NAME, 2);
        assertOffsetAccuracyForSynchronizedRecords();

        Struct updatedChildStruct = createNestedSetTimestampIncrementedStruct(childId, 1, 2, "Child", 1474661402000L);


        jdbcDbWriter.write(asList(
                new SinkRecord(TOPIC, 0, keySchema, rootId, NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, null, 2),
                new SinkRecord(TOPIC, 0, keySchema, childId, NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA, updatedChildStruct, 3)
        ));

        assertTableSize(NESTED_SET_LOG_TABLE_NAME, 4);
        assertTableSize(NESTED_SET_TABLE_NAME, 1);
        assertOffsetAccuracyForSynchronizedRecords();
    }

    private Struct createNestedSetIncrementedStruct(long id, int left, int right, String label) {
        return new Struct(NESTED_SET_INCREMENTED_SCHEMA)
                .put(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, id)
                .put(TABLE_LEFT_COLUMN_NAME_DEFAULT, left)
                .put(TABLE_RIGHT_COLUMN_NAME_DEFAULT, right)
                .put(TABLE_LABEL_COLUMN_NAME, label);
    }

    private Struct createNestedSetTimestampIncrementedStruct(long id, int left, int right, String label, long instantMilliseconds) {
        return new Struct(NESTED_SET_TIMESTAMP_INCREMENTED_SCHEMA)
                .put(TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT, id)
                .put(TABLE_LEFT_COLUMN_NAME_DEFAULT, left)
                .put(TABLE_RIGHT_COLUMN_NAME_DEFAULT, right)
                .put(TABLE_LABEL_COLUMN_NAME, label)
                .put(TABLE_MODIFIED_COLUMN_NAME, new Date(instantMilliseconds));
    }

    private void assertOffsetAccuracyForSynchronizedRecords() throws SQLException {
        long logOffset = getLogOffset();
        long maxNestedSetLogId = getMaxNestedSetLogId();
        assertThat("The recorded offset should match the max log_id of the tuples from " + NESTED_SET_LOG_TABLE_NAME + " table",
                logOffset, equalTo(maxNestedSetLogId));
    }


    private void assertTableSize(String tableName, int size) throws SQLException {
        jdbcHelper.select("select count(*) from " + tableName, rs -> {
            assertThat(rs.getInt(1), equalTo(size));
        });
    }

    private long getLogOffset() throws SQLException {
        AtomicLong offset = new AtomicLong(0);
        jdbcHelper.select("select log_table_offset from nested_set_sync_log_offset where log_table_name='" + NESTED_SET_LOG_TABLE_NAME + "'", rs -> {
            offset.set(rs.getLong(1));
        });
        return offset.get();
    }

    private long getMaxNestedSetLogId() throws SQLException {
        AtomicLong maxNestedSetLogId = new AtomicLong(0);
        jdbcHelper.select("select max(log_id) from " + NESTED_SET_LOG_TABLE_NAME, rs -> {
            maxNestedSetLogId.set(rs.getLong(1));
        });
        return maxNestedSetLogId.get();
    }
}

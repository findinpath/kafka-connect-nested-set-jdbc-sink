/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.util.DatabaseDialectRecommender;
import com.findinpath.connect.nestedset.jdbc.util.DeleteEnabledRecommender;
import com.findinpath.connect.nestedset.jdbc.util.EnumRecommender;
import com.findinpath.connect.nestedset.jdbc.util.PrimaryKeyModeRecommender;
import com.findinpath.connect.nestedset.jdbc.util.QuoteMethod;
import com.findinpath.connect.nestedset.jdbc.util.StringUtils;
import com.findinpath.connect.nestedset.jdbc.util.TableType;
import com.findinpath.connect.nestedset.jdbc.util.TimeZoneValidator;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

public class JdbcSinkConfig extends AbstractConfig {

  public enum InsertMode {
    INSERT,
    UPSERT,
    UPDATE;

  }

  public enum PrimaryKeyMode {
    NONE,
    KAFKA,
    RECORD_KEY,
    RECORD_VALUE;
  }

  public static final List<String> DEFAULT_KAFKA_PK_NAMES = Collections.unmodifiableList(
      Arrays.asList(
          "__connect_topic",
          "__connect_partition",
          "__connect_offset"
      )
  );

  public static final String CONNECTION_URL = "connection.url";
  private static final String CONNECTION_URL_DOC =
      "JDBC connection URL.\n"
          + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
          + "``jdbc:mysql://localhost/db_name``, "
          + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
          + "databaseName=db_name``";
  private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

  public static final String CONNECTION_USER = "connection.user";
  private static final String CONNECTION_USER_DOC = "JDBC connection user.";
  private static final String CONNECTION_USER_DISPLAY = "JDBC User";

  public static final String CONNECTION_PASSWORD = "connection.password";
  private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
  private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

  public static final String CONNECTION_ATTEMPTS_CONFIG = "connection.attempts";
  private static final String CONNECTION_ATTEMPTS_DOC
      = "Maximum number of attempts to retrieve a valid JDBC connection. "
      + "Must be a positive integer.";
  private static final String CONNECTION_ATTEMPTS_DISPLAY = "JDBC connection attempts";
  public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

  public static final String CONNECTION_BACKOFF_CONFIG = "connection.backoff.ms";
  private static final String CONNECTION_BACKOFF_DOC
      = "Backoff time in milliseconds between connection attempts.";
  private static final String CONNECTION_BACKOFF_DISPLAY
      = "JDBC connection backoff in milliseconds";
  public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

  public static final String TABLE_NAME = "table.name";
  private static final String TABLE_NAME_DOC =
      "The destination table name for the nested set data.";
  private static final String TABLE_NAME_DISPLAY = "Table Name";

  public static final String TABLE_LEFT_COLUMN_NAME = "table.left.column.name";
  private static final String TABLE_LEFT_COLUMN_NAME_DEFAULT = "lft";
  private static final String TABLE_LEFT_COLUMN_NAME_DOC =
          "The column name identifier for the left coordinate in the nested set table";
  private static final String TABLE_LEFT_COLUMN_NAME_DISPLAY = "Table Left Nested Set Coordinate Column Name";

  public static final String TABLE_RIGHT_COLUMN_NAME = "table.right.column.name";
  private static final String TABLE_RIGHT_COLUMN_NAME_DEFAULT = "rgt";
  private static final String TABLE_RIGHT_COLUMN_NAME_DOC =
          "The column name identifier for the right coordinate in the nested set table";
  private static final String TABLE_RIGHT_COLUMN_NAME_DISPLAY = "Table Right Nested Set Coordinate Column Name";

  public static final String LOG_TABLE_NAME = "log.table.name";
  private static final String LOG_TABLE_NAME_DEFAULT = "${topic}_log";
  private static final String LOG_TABLE_NAME_DOC =
          "The log table name for the nested set data. In this table are sinked the nested set entries from Kafka" +
                  "and they get subsequently synchronized in the destination table once the nested set merged structure" +
                  "between the log and the existing nested set data is valid.";
  private static final String LOG_TABLE_NAME_DISPLAY = "Log Table Name Format";

  public static final String LOG_TABLE_PRIMARY_KEY_COLUMN_NAME = "log.table.primary.key.column.name";
  private static final String LOG_TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT = "log_id";
  private static final String LOG_TABLE_PRIMARY_KEY_COLUMN_NAME_DOC =
          "The column name identifier for the autoincremented primary key in the log table";
  private static final String LOG_TABLE_PRIMARY_KEY_COLUMN_NAME_DISPLAY = "Log Table Primary Key Column Name";

  public static final String MAX_RETRIES = "max.retries";
  private static final int MAX_RETRIES_DEFAULT = 10;
  private static final String MAX_RETRIES_DOC =
      "The maximum number of times to retry on errors before failing the task.";
  private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

  public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final int RETRY_BACKOFF_MS_DEFAULT = 3000;
  private static final String RETRY_BACKOFF_MS_DOC =
      "The time in milliseconds to wait following an error before a retry attempt is made.";
  private static final String RETRY_BACKOFF_MS_DISPLAY = "Retry Backoff (millis)";

  public static final String BATCH_SIZE = "batch.size";
  private static final int BATCH_SIZE_DEFAULT = 3000;
  private static final String BATCH_SIZE_DOC =
      "Specifies how many records to attempt to batch together for insertion into the destination"
      + " table, when possible.";
  private static final String BATCH_SIZE_DISPLAY = "Batch Size";

  public static final String DELETE_ENABLED = "delete.enabled";
  private static final String DELETE_ENABLED_DEFAULT = "false";
  private static final String DELETE_ENABLED_DOC =
      "Whether to treat ``null`` record values as deletes. Requires ``pk.mode`` "
      + "to be ``record_key``.";
  private static final String DELETE_ENABLED_DISPLAY = "Enable deletes";

  public static final String AUTO_CREATE = "auto.create";
  private static final String AUTO_CREATE_DEFAULT = "false";
  private static final String AUTO_CREATE_DOC =
      "Whether to automatically create the destination table based on record schema if it is "
      + "found to be missing by issuing ``CREATE``.";
  private static final String AUTO_CREATE_DISPLAY = "Auto-Create";

  public static final String AUTO_EVOLVE = "auto.evolve";
  private static final String AUTO_EVOLVE_DEFAULT = "false";
  private static final String AUTO_EVOLVE_DOC =
      "Whether to automatically add columns in the table schema when found to be missing relative "
      + "to the record schema by issuing ``ALTER``.";
  private static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

  public static final String PK_FIELDS = "pk.fields";
  private static final String PK_FIELDS_DEFAULT = "";
  private static final String PK_FIELDS_DOC =
      "List of comma-separated primary key field names. The runtime interpretation of this config"
      + " depends on the ``pk.mode``:\n"
      + "``none``\n"
      + "    Ignored as no fields are used as primary key in this mode.\n"
      + "``kafka``\n"
      + "    Must be a trio representing the Kafka coordinates, defaults to ``"
      + StringUtils.join(DEFAULT_KAFKA_PK_NAMES, ",") + "`` if empty.\n"
      + "``record_key``\n"
      + "    If empty, all fields from the key struct will be used, otherwise used to extract the"
      + " desired fields - for primitive key only a single field name must be configured.\n"
      + "``record_value``\n"
      + "    If empty, all fields from the value struct will be used, otherwise used to extract "
      + "the desired fields.";
  private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";

  public static final String PK_MODE = "pk.mode";
  private static final String PK_MODE_DEFAULT = "none";
  private static final String PK_MODE_DOC =
      "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. "
      + "Supported modes are:\n"
      + "``none``\n"
      + "    No keys utilized.\n"
      + "``kafka``\n"
      + "    Kafka coordinates are used as the PK.\n"
      + "``record_key``\n"
      + "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
      + "``record_value``\n"
      + "    Field(s) from the record value are used, which must be a struct.";
  private static final String PK_MODE_DISPLAY = "Primary Key Mode";

  public static final String FIELDS_WHITELIST = "fields.whitelist";
  private static final String FIELDS_WHITELIST_DEFAULT = "";
  private static final String FIELDS_WHITELIST_DOC =
      "List of comma-separated record value field names. If empty, all fields from the record "
      + "value are utilized, otherwise used to filter to the desired fields.\n"
      + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field"
      + "(s) form the primary key columns in the destination database,"
      + " while this configuration is applicable for the other columns.";
  private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";

  private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);

  private static final String CONNECTION_GROUP = "Connection";
  private static final String WRITES_GROUP = "Writes";
  private static final String DATAMAPPING_GROUP = "Data Mapping";
  private static final String DDL_GROUP = "DDL Support";
  private static final String RETRIES_GROUP = "Retries";

  public static final String DIALECT_NAME_CONFIG = "dialect.name";
  private static final String DIALECT_NAME_DISPLAY = "Database Dialect";
  public static final String DIALECT_NAME_DEFAULT = "";
  private static final String DIALECT_NAME_DOC =
      "The name of the database dialect that should be used for this connector. By default this "
      + "is empty, and the connector automatically determines the dialect based upon the "
      + "JDBC connection URL. Use this if you want to override that behavior and use a "
      + "specific dialect. All properly-packaged dialects in the JDBC connector plugin "
      + "can be used.";

  public static final String DB_TIMEZONE_CONFIG = "db.timezone";
  public static final String DB_TIMEZONE_DEFAULT = "UTC";
  private static final String DB_TIMEZONE_CONFIG_DOC =
      "Name of the JDBC timezone that should be used in the connector when "
      + "inserting time-based values. Defaults to UTC.";
  private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB Time Zone";

  public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";
  public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT = QuoteMethod.ALWAYS.name().toString();
  public static final String QUOTE_SQL_IDENTIFIERS_DOC =
      "When to quote table names, column names, and other identifiers in SQL statements. "
          + "For backward compatibility, the default is ``always``.";
  private static final String QUOTE_SQL_IDENTIFIERS_DISPLAY =
      "Quote Identifiers";

  public static final String TABLE_TYPES_CONFIG = "table.types";
  private static final String TABLE_TYPES_DISPLAY = "Table Types";
  public static final String TABLE_TYPES_DEFAULT = TableType.TABLE.toString();
  private static final String TABLE_TYPES_DOC =
      "The comma-separated types of database tables to which the sink connector can write. "
      + "By default this is ``" + TableType.TABLE + "``, but any combination of ``"
      + TableType.TABLE + "`` and ``" + TableType.VIEW + "`` is allowed. Not all databases "
      + "support writing to views, and when they do the the sink connector will fail if the "
      + "view definition does not match the records' schemas (regardless of ``"
      + AUTO_EVOLVE + "``).";

  private static final EnumRecommender QUOTE_METHOD_RECOMMENDER =
      EnumRecommender.in(QuoteMethod.values());

  private static final EnumRecommender TABLE_TYPES_RECOMMENDER =
      EnumRecommender.in(TableType.values());

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
        // Connection
        .define(
            CONNECTION_URL,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            CONNECTION_URL_DOC,
            CONNECTION_GROUP,
            1,
            ConfigDef.Width.LONG,
            CONNECTION_URL_DISPLAY
        )
        .define(
            CONNECTION_USER,
            ConfigDef.Type.STRING,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_USER_DOC,
            CONNECTION_GROUP,
            2,
            ConfigDef.Width.MEDIUM,
            CONNECTION_USER_DISPLAY
        )
        .define(
            CONNECTION_PASSWORD,
            ConfigDef.Type.PASSWORD,
            null,
            ConfigDef.Importance.HIGH,
            CONNECTION_PASSWORD_DOC,
            CONNECTION_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            CONNECTION_PASSWORD_DISPLAY
        )
        .define(
            DIALECT_NAME_CONFIG,
            ConfigDef.Type.STRING,
            DIALECT_NAME_DEFAULT,
            DatabaseDialectRecommender.INSTANCE,
            ConfigDef.Importance.LOW,
            DIALECT_NAME_DOC,
            CONNECTION_GROUP,
            4,
            ConfigDef.Width.LONG,
            DIALECT_NAME_DISPLAY,
            DatabaseDialectRecommender.INSTANCE
        )
        .define(
            CONNECTION_ATTEMPTS_CONFIG,
            Type.INT,
            CONNECTION_ATTEMPTS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            Importance.LOW,
            CONNECTION_ATTEMPTS_DOC,
            CONNECTION_GROUP,
            5,
            Width.SHORT,
            CONNECTION_ATTEMPTS_DISPLAY
        )
        .define(
            CONNECTION_BACKOFF_CONFIG,
            Type.LONG,
            CONNECTION_BACKOFF_DEFAULT,
            Importance.LOW,
            CONNECTION_BACKOFF_DOC,
            CONNECTION_GROUP,
            6,
            Width.SHORT,
            CONNECTION_BACKOFF_DISPLAY
        )
        // Writes
        .define(
            BATCH_SIZE,
            ConfigDef.Type.INT,
            BATCH_SIZE_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            BATCH_SIZE_DOC, WRITES_GROUP,
            1,
            ConfigDef.Width.SHORT,
            BATCH_SIZE_DISPLAY
        )
        .define(
            DELETE_ENABLED,
            ConfigDef.Type.BOOLEAN,
            DELETE_ENABLED_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            DELETE_ENABLED_DOC, WRITES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            DELETE_ENABLED_DISPLAY,
            DeleteEnabledRecommender.INSTANCE
        )
        .define(
            TABLE_TYPES_CONFIG,
            ConfigDef.Type.LIST,
            TABLE_TYPES_DEFAULT,
            TABLE_TYPES_RECOMMENDER,
            ConfigDef.Importance.LOW,
            TABLE_TYPES_DOC,
            WRITES_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            TABLE_TYPES_DISPLAY
        )
        // Data Mapping
        .define(
            TABLE_NAME,
            ConfigDef.Type.STRING,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.MEDIUM,
            TABLE_NAME_DOC,
            DATAMAPPING_GROUP,
            1,
            ConfigDef.Width.LONG,
            TABLE_NAME_DISPLAY
        )
        .define(
            LOG_TABLE_NAME,
            ConfigDef.Type.STRING,
            LOG_TABLE_NAME_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            LOG_TABLE_NAME_DOC,
            DATAMAPPING_GROUP,
            2,
            ConfigDef.Width.LONG,
            LOG_TABLE_NAME_DISPLAY
        )
        .define(
            PK_MODE,
            ConfigDef.Type.STRING,
            PK_MODE_DEFAULT,
            EnumValidator.in(PrimaryKeyMode.values()),
            ConfigDef.Importance.HIGH,
            PK_MODE_DOC,
            DATAMAPPING_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            PK_MODE_DISPLAY,
            PrimaryKeyModeRecommender.INSTANCE
        )
        .define(
            PK_FIELDS,
            ConfigDef.Type.LIST,
            PK_FIELDS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            PK_FIELDS_DOC,
            DATAMAPPING_GROUP,
            4,
            ConfigDef.Width.LONG, PK_FIELDS_DISPLAY
        )
        .define(
            FIELDS_WHITELIST,
            ConfigDef.Type.LIST,
            FIELDS_WHITELIST_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            FIELDS_WHITELIST_DOC,
            DATAMAPPING_GROUP,
            5,
            ConfigDef.Width.LONG,
            FIELDS_WHITELIST_DISPLAY
        ).define(
          DB_TIMEZONE_CONFIG,
          ConfigDef.Type.STRING,
          DB_TIMEZONE_DEFAULT,
          TimeZoneValidator.INSTANCE,
          ConfigDef.Importance.MEDIUM,
          DB_TIMEZONE_CONFIG_DOC,
          DATAMAPPING_GROUP,
          6,
          ConfigDef.Width.MEDIUM,
          DB_TIMEZONE_CONFIG_DISPLAY
        )
        // DDL
        .define(
            AUTO_CREATE,
            ConfigDef.Type.BOOLEAN,
            AUTO_CREATE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_CREATE_DOC, DDL_GROUP,
            1,
            ConfigDef.Width.SHORT,
            AUTO_CREATE_DISPLAY
        )
        .define(
            AUTO_EVOLVE,
            ConfigDef.Type.BOOLEAN,
            AUTO_EVOLVE_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            AUTO_EVOLVE_DOC, DDL_GROUP,
            2,
            ConfigDef.Width.SHORT,
            AUTO_EVOLVE_DISPLAY
        ).define(
            QUOTE_SQL_IDENTIFIERS_CONFIG,
            ConfigDef.Type.STRING,
            QUOTE_SQL_IDENTIFIERS_DEFAULT,
            ConfigDef.Importance.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DOC,
            DDL_GROUP,
            3,
            ConfigDef.Width.MEDIUM,
            QUOTE_SQL_IDENTIFIERS_DISPLAY,
            QUOTE_METHOD_RECOMMENDER
        )
        .define(
            TABLE_LEFT_COLUMN_NAME,
            ConfigDef.Type.STRING,
            TABLE_LEFT_COLUMN_NAME_DEFAULT,
            ConfigDef.Importance.HIGH,
            TABLE_LEFT_COLUMN_NAME_DOC,
            DATAMAPPING_GROUP,
            4,
            ConfigDef.Width.LONG,
            TABLE_LEFT_COLUMN_NAME_DISPLAY
        )
        .define(
            TABLE_RIGHT_COLUMN_NAME,
            ConfigDef.Type.STRING,
            TABLE_RIGHT_COLUMN_NAME_DEFAULT,
            ConfigDef.Importance.HIGH,
            TABLE_RIGHT_COLUMN_NAME_DOC,
            DATAMAPPING_GROUP,
            5,
            ConfigDef.Width.LONG,
            TABLE_RIGHT_COLUMN_NAME_DISPLAY
        )
        .define(
            LOG_TABLE_PRIMARY_KEY_COLUMN_NAME,
            ConfigDef.Type.STRING,
            LOG_TABLE_PRIMARY_KEY_COLUMN_NAME_DEFAULT,
            ConfigDef.Importance.HIGH,
            LOG_TABLE_PRIMARY_KEY_COLUMN_NAME_DOC,
            DATAMAPPING_GROUP,
            6,
            ConfigDef.Width.LONG,
            LOG_TABLE_PRIMARY_KEY_COLUMN_NAME_DISPLAY
        )
          // Retries
        .define(
            MAX_RETRIES,
            ConfigDef.Type.INT,
            MAX_RETRIES_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            MAX_RETRIES_DOC,
            RETRIES_GROUP,
            1,
            ConfigDef.Width.SHORT,
            MAX_RETRIES_DISPLAY
        )
        .define(
            RETRY_BACKOFF_MS,
            ConfigDef.Type.INT,
            RETRY_BACKOFF_MS_DEFAULT,
            NON_NEGATIVE_INT_VALIDATOR,
            ConfigDef.Importance.MEDIUM,
            RETRY_BACKOFF_MS_DOC,
            RETRIES_GROUP,
            2,
            ConfigDef.Width.SHORT,
            RETRY_BACKOFF_MS_DISPLAY
        );

  public final String connectionUrl;
  public final String connectionUser;
  public final String connectionPassword;
  public final String tableName;
  public final String logTableName;
  public final int batchSize;
  public final boolean deleteEnabled;
  public final int maxRetries;
  public final int retryBackoffMs;
  public final boolean autoCreate;
  public final boolean autoEvolve;
  public final String tableLeftColumnName;
  public final String tableRightColumnName;
  public final String logTablePrimaryKeyColumnName;
  public final PrimaryKeyMode pkMode;
  public final List<String> pkFields;
  public final Set<String> fieldsWhitelist;
  public final String dialectName;
  public final TimeZone timeZone;
  public final EnumSet<TableType> tableTypes;

  public JdbcSinkConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    connectionUrl = getString(CONNECTION_URL);
    connectionUser = getString(CONNECTION_USER);
    connectionPassword = getPasswordValue(CONNECTION_PASSWORD);
    tableName = getString(TABLE_NAME).trim();
    logTableName = getString(LOG_TABLE_NAME).trim();
    batchSize = getInt(BATCH_SIZE);
    deleteEnabled = getBoolean(DELETE_ENABLED);
    maxRetries = getInt(MAX_RETRIES);
    retryBackoffMs = getInt(RETRY_BACKOFF_MS);
    autoCreate = getBoolean(AUTO_CREATE);
    autoEvolve = getBoolean(AUTO_EVOLVE);
    tableLeftColumnName = getString(TABLE_LEFT_COLUMN_NAME).trim();
    tableRightColumnName = getString(TABLE_RIGHT_COLUMN_NAME).trim();
    logTablePrimaryKeyColumnName = getString(LOG_TABLE_PRIMARY_KEY_COLUMN_NAME).trim();
    pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
    pkFields = getList(PK_FIELDS);
    dialectName = getString(DIALECT_NAME_CONFIG);
    fieldsWhitelist = new HashSet<>(getList(FIELDS_WHITELIST));
    String dbTimeZone = getString(DB_TIMEZONE_CONFIG);
    timeZone = TimeZone.getTimeZone(ZoneId.of(dbTimeZone));

    if (deleteEnabled && pkMode != PrimaryKeyMode.RECORD_KEY) {
      throw new ConfigException(
          "Primary key mode must be 'record_key' when delete support is enabled");
    }
    tableTypes = TableType.parse(getList(TABLE_TYPES_CONFIG));
  }

  private String getPasswordValue(String key) {
    Password password = getPassword(key);
    if (password != null) {
      return password.value();
    }
    return null;
  }

  public EnumSet<TableType> tableTypes() {
    return tableTypes;
  }

  public Set<String> tableTypeNames() {
    return tableTypes().stream().map(TableType::toString).collect(Collectors.toSet());
  }

  private static class EnumValidator implements ConfigDef.Validator {
    private final List<String> canonicalValues;
    private final Set<String> validValues;

    private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
      this.canonicalValues = canonicalValues;
      this.validValues = validValues;
    }

    public static <E> EnumValidator in(E[] enumerators) {
      final List<String> canonicalValues = new ArrayList<>(enumerators.length);
      final Set<String> validValues = new HashSet<>(enumerators.length * 2);
      for (E e : enumerators) {
        canonicalValues.add(e.toString().toLowerCase());
        validValues.add(e.toString().toUpperCase());
        validValues.add(e.toString().toLowerCase());
      }
      return new EnumValidator(canonicalValues, validValues);
    }

    @Override
    public void ensureValid(String key, Object value) {
      if (!validValues.contains(value)) {
        throw new ConfigException(key, value, "Invalid enumerator");
      }
    }

    @Override
    public String toString() {
      return canonicalValues.toString();
    }
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toEnrichedRst());
  }

}

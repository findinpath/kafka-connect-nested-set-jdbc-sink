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

package com.findinpath.connect.nestedset.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.findinpath.connect.nestedset.jdbc.util.Version;
import com.findinpath.testcontainers.KafkaConnectContainer;
import com.findinpath.testcontainers.KafkaContainer;
import com.findinpath.testcontainers.SchemaRegistryContainer;
import com.findinpath.testcontainers.ZookeeperContainer;
import com.findinpath.kafka.connect.model.ConnectorConfiguration;
import io.restassured.http.ContentType;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import static com.findinpath.testcontainers.KafkaConnectContainer.PLUGIN_PATH_CONTAINER;
import static io.restassured.RestAssured.given;
import static java.lang.String.format;

/**
 * Abstract class used for testing the accuracy of syncing
 * a nested set model via kafka-connect-jdbc.
 * <p>
 * This class groups the complexity related to setting up before all the tests:
 *
 * <ul>
 *     <li>Apache Kafka ecosystem test containers</li>
 *     <li>PostgreSQL source database test container</li>
 *     <li>PostgreSQL sink database test container</li>
 * </ul>
 * <p>
 * and also setting up before each test:
 * <ul>
 *     <li>truncate the content of the source and sink databases</li>
 *     <li>kafka-connect-jdbc connector for the source <code>nested_set_node</code> table</li>
 * </ul>
 * and tearing down after each test:
 * <ul>
 *     <li>kafka-connect-jdbc connector for the source <code>nested_set_node</code> table</li>
 * </ul>
 */
public abstract class AbstractNestedSetSyncTest {

    private static final String KAFKA_CONNECT_CONNECTOR_NAME = "findinpath";
    private static final String NESTED_SET_NODE_SOURCE_TABLE_NAME = "nested_set_node";


    private static final String NESTED_SET_NODE_SINK_TABLE_NAME = "nested_set_node";
    private static final String NESTED_SET_NODE_SINK_LOG_TABLE_NAME = "nested_set_node_log";
    private static final String NESTED_SET_NODE_SINK_LOG_OFFSET_TABLE_NAME = "nested_set_node_log_offset";

    private static final String POSTGRES_DB_DRIVER_CLASS_NAME = "org.postgresql.Driver";

    protected static final String POSTGRES_SOURCE_DB_NAME = "source";
    protected static final String POSTGRES_SOURCE_NETWORK_ALIAS = "source";
    protected static final String POSTGRES_SOURCE_DB_USERNAME = "sa";
    protected static final String POSTGRES_SOURCE_DB_PASSWORD = "p@ssw0rd!source";
    protected static final int POSTGRES_INTERNAL_PORT = 5432;

    private static final String TRUNCATE_SOURCE_NESTED_SET_NODE_SQL =
            "TRUNCATE nested_set_node";

    protected static final String POSTGRES_SINK_DB_NAME = "sink";
    protected static final String POSTGRES_SINK_NETWORK_ALIAS = "sink";
    protected static final String POSTGRES_SINK_DB_USERNAME = "sa";
    protected static final String POSTGRES_SINK_DB_PASSWORD = "p@ssw0rd!sink";

    private static final String TRUNCATE_SINK_NESTED_SET_NODE_SQL =
            "TRUNCATE nested_set_node";

    private static final String TRUNCATE_SINK_NESTED_SET_NODE_LOG_SQL =
            "TRUNCATE nested_set_node_log";

    private static final String TRUNCATE_SINK_LOG_OFFSET_SQL =
            "TRUNCATE nested_set_node_log_offset";

    /**
     * Postgres JDBC connection URL to be used within the docker environment.
     */
    private static final String POSTGRES_SOURCE_INTERNAL_CONNECTION_URL = format("jdbc:postgresql://%s:%d/%s?loggerLevel=OFF",
            POSTGRES_SOURCE_NETWORK_ALIAS,
            POSTGRES_INTERNAL_PORT,
            POSTGRES_SOURCE_DB_NAME);

    private static final String POSTGRES_SINK_INTERNAL_CONNECTION_URL = format("jdbc:postgresql://%s:%d/%s?loggerLevel=OFF",
            POSTGRES_SINK_NETWORK_ALIAS,
            POSTGRES_INTERNAL_PORT,
            POSTGRES_SINK_DB_NAME);

    private static Network network;

    protected static ZookeeperContainer zookeeperContainer;
    protected static KafkaContainer kafkaContainer;
    protected static SchemaRegistryContainer schemaRegistryContainer;
    protected static KafkaConnectContainer kafkaConnectContainer;

    protected static PostgreSQLContainer sourcePostgreSQLContainer;
    protected static PostgreSQLContainer sinkPostgreSQLContainer;

    private static final ObjectMapper mapper = new ObjectMapper();

    private String testUuid;


    /**
     * Bootstrap the docker instances needed for interacting with :
     * <ul>
     *     <li>Confluent Kafka ecosystem</li>
     *     <li>PostgreSQL source</li>
     *     <li>PostgreSQL sink</li>
     * </ul>
     * <p>
     * and subsequently register the kafka-connect-jdbc connector on the
     * source nested_set_node database table.
     */
    @BeforeAll
    public static void dockerSetup() throws Exception {
        network = Network.newNetwork();

        // Confluent setup
        zookeeperContainer = new ZookeeperContainer()
                .withNetwork(network);
        kafkaContainer = new KafkaContainer(zookeeperContainer.getInternalUrl())
                .withNetwork(network);
        schemaRegistryContainer = new SchemaRegistryContainer(zookeeperContainer.getInternalUrl())
                .withNetwork(network);
        kafkaConnectContainer = new KafkaConnectContainer(kafkaContainer.getInternalBootstrapServersUrl())
                .withNetwork(network)
                .withPlugin(Paths.get("target/kafka-connect-nested-set-jdbc-sink-"+ Version.getVersion() +".jar"), PLUGIN_PATH_CONTAINER+"/kafka-connect-jdbc")
                .withKeyConverter("org.apache.kafka.connect.storage.StringConverter")
                .withValueConverter("io.confluent.connect.avro.AvroConverter")
                .withSchemaRegistryUrl(schemaRegistryContainer.getInternalUrl());

        // Postgres setup
        sourcePostgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_SOURCE_NETWORK_ALIAS)
                .withInitScript("endtoend/source/postgres/init_postgres.sql")
                .withDatabaseName(POSTGRES_SOURCE_DB_NAME)
                .withUsername(POSTGRES_SOURCE_DB_USERNAME)
                .withPassword(POSTGRES_SOURCE_DB_PASSWORD);


        sinkPostgreSQLContainer = new PostgreSQLContainer<>("postgres:12")
                .withNetwork(network)
                .withNetworkAliases(POSTGRES_SINK_NETWORK_ALIAS)
                .withInitScript("endtoend/sink/postgres/init_postgres.sql")
                .withDatabaseName(POSTGRES_SINK_DB_NAME)
                .withUsername(POSTGRES_SINK_DB_USERNAME)
                .withPassword(POSTGRES_SINK_DB_PASSWORD);

        Startables
                .deepStart(Stream.of(zookeeperContainer,
                        kafkaContainer,
                        schemaRegistryContainer,
                        kafkaConnectContainer,
                        sourcePostgreSQLContainer,
                        sinkPostgreSQLContainer)
                )
                .join();

        verifyKafkaConnectHealth();
    }

    protected void setup() throws Exception{
        truncateSourceTables();
        truncateSinkTables();

        testUuid = UUID.randomUUID().toString();

        setupKakfaConnectJdbcNestedSetNodeSourceTableConnector(testUuid);
        setupKakfaConnectJdbcNestedSetNodeSinkConnector(testUuid);
    }


    protected void tearDown() {
        deleteKakfaConnectJdbcNestedSetNodeSourceConnector(testUuid);
        deleteKakfaConnectJdbcNestedSetNodeSinkConnector(testUuid);
    }

    protected String getKafkaConnectOutputTopic() {
        return getKafkaConnectOutputTopicPrefix(testUuid) + NESTED_SET_NODE_SOURCE_TABLE_NAME;
    }

    private static void setupKakfaConnectJdbcNestedSetNodeSourceTableConnector(String testUuid) {
        Map<String, String> config = new HashMap<>();
        String connectorName = getKafkaConnectorSourceName(testUuid);
        config.put("name", connectorName);
        config.put("connector.class", "io.confluent.connect.jdbc.JdbcSourceConnector");
        config.put("tasks.max", "1");
        config.put("connection.url", POSTGRES_SOURCE_INTERNAL_CONNECTION_URL);
        config.put("connection.user", POSTGRES_SOURCE_DB_USERNAME);
        config.put("connection.password", POSTGRES_SOURCE_DB_PASSWORD);
        config.put("table.whitelist", NESTED_SET_NODE_SOURCE_TABLE_NAME);
        config.put("mode", "timestamp+incrementing");
        config.put("timestamp.column.name", "updated");
        config.put("validate.non.null", "false");
        config.put("incrementing.column.name", "id");
        config.put("topic.prefix", getKafkaConnectOutputTopicPrefix(testUuid));

        ConnectorConfiguration nestedSetNodeSourceConnectorConfig = new ConnectorConfiguration(connectorName, config);
        registerConnector(nestedSetNodeSourceConnectorConfig);
    }

    private static void setupKakfaConnectJdbcNestedSetNodeSinkConnector(String testUuid) {
        Map<String, String> config = new HashMap<>();
        String connectorName = getKafkaConnectorSinkName(testUuid);
        config.put("name", connectorName);
        config.put("connector.class", "com.findinpath.connect.nestedset.jdbc.NestedSetJdbcSinkConnector");
        config.put("tasks.max", "1");
        config.put("topics", getKafkaConnectOutputTopicPrefix(testUuid) + NESTED_SET_NODE_SOURCE_TABLE_NAME);
        config.put("connection.url", POSTGRES_SINK_INTERNAL_CONNECTION_URL);
        config.put("connection.user", POSTGRES_SINK_DB_USERNAME);
        config.put("connection.password", POSTGRES_SINK_DB_PASSWORD);
        config.put("table.name", NESTED_SET_NODE_SINK_TABLE_NAME);
        config.put("log.table.name", NESTED_SET_NODE_SINK_LOG_TABLE_NAME);
        config.put("log.offset.table.name", NESTED_SET_NODE_SINK_LOG_OFFSET_TABLE_NAME);

        ConnectorConfiguration nestedSetNodeSourceConnectorConfig = new ConnectorConfiguration(connectorName, config);
        registerConnector(nestedSetNodeSourceConnectorConfig);
    }

    private static void deleteKakfaConnectJdbcNestedSetNodeSourceConnector(String testUuid) {
        given()
                .log().uri()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .when()
                .delete(kafkaConnectContainer.getUrl() + "/connectors/" + getKafkaConnectorSourceName(testUuid))
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    private static void deleteKakfaConnectJdbcNestedSetNodeSinkConnector(String testUuid) {
        given()
                .log().uri()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .when()
                .delete(kafkaConnectContainer.getUrl() + "/connectors/" + getKafkaConnectorSinkName(testUuid))
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }

    private static String getKafkaConnectorSourceName(String suffix) {
        return KAFKA_CONNECT_CONNECTOR_NAME + "-source-"  + suffix;
    }

    private static String getKafkaConnectorSinkName(String suffix) {
        return KAFKA_CONNECT_CONNECTOR_NAME + "-sink-"  + suffix;
    }

    private static String getKafkaConnectOutputTopicPrefix(String extraPrefix) {
        return KAFKA_CONNECT_CONNECTOR_NAME + "." + extraPrefix + ".";
    }

    private static String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static void registerConnector(ConnectorConfiguration connectorConfiguration) {
        given()
                .log().all()
                .contentType(ContentType.JSON)
                .accept(ContentType.JSON)
                .body(toJson(connectorConfiguration))
                .when()
                .post(kafkaConnectContainer.getUrl() + "/connectors")
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_CREATED);
    }

    /**
     * Simple HTTP check to see that the kafka-connect server is available.
     */
    private static void verifyKafkaConnectHealth() {
        given()
                .log().headers()
                .contentType(ContentType.JSON)
                .when()
                .get(kafkaConnectContainer.getUrl())
                .andReturn()
                .then()
                .log().all()
                .statusCode(HttpStatus.SC_OK);
    }


    private static Connection getConnection(String jdbcUrl, String username, String password) throws SQLException{
        Properties properties = new Properties();
        if (username != null) {
            properties.setProperty("user", username);
        }
        if (password != null) {
            properties.setProperty("password", password);
        }
        return DriverManager.getConnection(jdbcUrl, properties);
    }

    private void truncateSourceTables() throws SQLException{
        try (Connection connection = getConnection(sourcePostgreSQLContainer.getJdbcUrl(), POSTGRES_SOURCE_DB_USERNAME, POSTGRES_SOURCE_DB_PASSWORD);
             PreparedStatement pstmt = connection.prepareStatement(TRUNCATE_SOURCE_NESTED_SET_NODE_SQL)) {
            pstmt.executeUpdate();
        }
    }

    private void truncateSinkTables() throws SQLException{
        try (Connection connection = getConnection(sinkPostgreSQLContainer.getJdbcUrl(), POSTGRES_SINK_DB_USERNAME, POSTGRES_SINK_DB_PASSWORD);
             PreparedStatement pstmtNestedSetNode = connection.prepareStatement(TRUNCATE_SINK_NESTED_SET_NODE_SQL);
             PreparedStatement pstmtNestedSetNodeLog = connection.prepareStatement(TRUNCATE_SINK_NESTED_SET_NODE_LOG_SQL);
             PreparedStatement pstmtNestedSetNodeLogOffset = connection.prepareStatement(TRUNCATE_SINK_LOG_OFFSET_SQL);
        ) {
            pstmtNestedSetNode.executeUpdate();
            pstmtNestedSetNodeLog.executeUpdate();
            pstmtNestedSetNodeLogOffset.executeUpdate();
        }
    }
}

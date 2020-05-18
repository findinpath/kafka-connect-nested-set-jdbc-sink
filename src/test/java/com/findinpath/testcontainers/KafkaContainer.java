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

package com.findinpath.testcontainers;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.TestcontainersConfiguration;

import java.util.HashMap;
import java.util.Map;

import static com.findinpath.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.testcontainers.Utils.containerLogsConsumer;
import static com.findinpath.testcontainers.Utils.getRandomFreePort;
import static java.lang.String.format;

/**
 * This class is a testcontainers Confluent implementation for the
 * Apache Kafka docker container.
 */
public class KafkaContainer extends GenericContainer<KafkaContainer> {
    private static final int KAFKA_INTERNAL_PORT = 9092;
    private static final int KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT = 29092;

    private final String networkAlias = "kafka";
    private final int exposedPort;
    private final String bootstrapServersUrl;
    private final String internalBootstrapServersUrl;

    public KafkaContainer(String zookeeperConnect) {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public KafkaContainer(String confluentPlatformVersion, String zookeeperConnect) {
        super(getKafkaContainerImage(confluentPlatformVersion));

        this.exposedPort = getRandomFreePort();

        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BROKER_ID", "1");
        env.put("KAFKA_ZOOKEEPER_CONNECT", zookeeperConnect);
        env.put("ZOOKEEPER_SASL_ENABLED", "false");
        env.put("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        env.put("KAFKA_LISTENERS",
                "PLAINTEXT://0.0.0.0:" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT +
                ",PLAINTEXT_HOST://0.0.0.0:" + KAFKA_INTERNAL_PORT);
        env.put("KAFKA_ADVERTISED_LISTENERS",
                "PLAINTEXT://" + networkAlias + ":" + KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT
                        + ",PLAINTEXT_HOST://" + getContainerIpAddress() + ":" + exposedPort);
        env.put("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT");
        env.put("KAFKA_SASL_ENABLED_MECHANISMS", "PLAINTEXT");
        env.put("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT");
        env.put("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAINTEXT");

        withLogConsumer(containerLogsConsumer(logger()));

        withEnv(env);
        withNetworkAliases(networkAlias);
        addFixedExposedPort(exposedPort, KAFKA_INTERNAL_PORT);


        this.bootstrapServersUrl = format("%s:%d", getContainerIpAddress(), exposedPort);
        this.internalBootstrapServersUrl = format("%s:%d", networkAlias, KAFKA_INTERNAL_ADVERTISED_LISTENERS_PORT);
    }

    private static String getKafkaContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "kafka.container.image",
                        "confluentinc/cp-kafka:" + confluentPlatformVersion
                );
    }

    /**
     * Get the url.
     *
     * @return
     */
    public String getBootstrapServersUrl() {
        return bootstrapServersUrl;
    }

    /**
     * Get the local url
     *
     * @return
     */
    public String getInternalBootstrapServersUrl() {
        return internalBootstrapServersUrl;
    }
}
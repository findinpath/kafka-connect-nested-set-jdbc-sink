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

package com.findinpath.connect.nestedset.jdbc.testcontainers;


import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.TestcontainersConfiguration;

import static com.findinpath.connect.nestedset.jdbc.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.connect.nestedset.jdbc.testcontainers.Utils.containerLogsConsumer;
import static java.lang.String.format;

/**
 * This class is a testcontainers implementation for the
 * <a href="Confluent Schema Registry">https://docs.confluent.io/current/schema-registry/index.html</a>
 * Docker container.
 */
public class SchemaRegistryContainer extends GenericContainer<SchemaRegistryContainer> {
    private static final int SCHEMA_REGISTRY_INTERNAL_PORT = 8081;


    private final String networkAlias = "schema-registry";

    public SchemaRegistryContainer(String zookeeperConnect) {
        this(CONFLUENT_PLATFORM_VERSION, zookeeperConnect);
    }

    public SchemaRegistryContainer(String confluentPlatformVersion, String zookeeperUrl) {
        super(getSchemaRegistryContainerImage(confluentPlatformVersion));

        addEnv("SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL", zookeeperUrl);
        addEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost");
        withLogConsumer(containerLogsConsumer(logger()));
        withExposedPorts(SCHEMA_REGISTRY_INTERNAL_PORT);
        withNetworkAliases(networkAlias);

        waitingFor(Wait.forHttp("/subjects"));

    }

    private static String getSchemaRegistryContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "schemaregistry.container.image",
                        "confluentinc/cp-schema-registry:" + confluentPlatformVersion
                );
    }

    /**
     * Get the url.
     *
     * @return
     */
    public String getUrl() {
        return format("http://%s:%d", this.getContainerIpAddress(), this.getMappedPort(SCHEMA_REGISTRY_INTERNAL_PORT));
    }

    /**
     * Get the local url
     *
     * @return
     */
    public String getInternalUrl() {
        return format("http://%s:%d", networkAlias, SCHEMA_REGISTRY_INTERNAL_PORT);
    }
}


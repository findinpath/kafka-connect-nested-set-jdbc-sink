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
import org.testcontainers.utility.TestcontainersConfiguration;

import static com.findinpath.connect.nestedset.jdbc.testcontainers.Utils.CONFLUENT_PLATFORM_VERSION;
import static com.findinpath.connect.nestedset.jdbc.testcontainers.Utils.containerLogsConsumer;
import static java.lang.String.format;

/**
 * This class is a testcontainers Confluent implementation for the
 * Apache Zookeeper docker container.
 */
public class ZookeeperContainer extends GenericContainer<ZookeeperContainer> {

    private static final int ZOOKEEPER_INTERNAL_PORT = 2181;
    private static final int ZOOKEEPER_TICK_TIME = 2000;

    private final String networkAlias = "zookeeper";

    public ZookeeperContainer() {
        this(CONFLUENT_PLATFORM_VERSION);
    }

    public ZookeeperContainer(String confluentPlatformVersion) {
        super(getZookeeperContainerImage(confluentPlatformVersion));

        addEnv("ZOOKEEPER_CLIENT_PORT", Integer.toString(ZOOKEEPER_INTERNAL_PORT));
        addEnv("ZOOKEEPER_TICK_TIME", Integer.toString(ZOOKEEPER_TICK_TIME));
        withLogConsumer(containerLogsConsumer(logger()));

        addExposedPort(ZOOKEEPER_INTERNAL_PORT);
        withNetworkAliases(networkAlias);
    }

    private static String getZookeeperContainerImage(String confluentPlatformVersion) {
        return (String) TestcontainersConfiguration
                .getInstance().getProperties().getOrDefault(
                        "zookeeper.container.image",
                        "confluentinc/cp-zookeeper:" + confluentPlatformVersion
                );
    }

    /**
     * Get the local url
     *
     * @return
     */
    public String getInternalUrl() {
        return format("%s:%d", networkAlias, ZOOKEEPER_INTERNAL_PORT);
    }
}

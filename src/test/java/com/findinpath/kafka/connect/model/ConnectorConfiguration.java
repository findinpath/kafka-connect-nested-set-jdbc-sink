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

package com.findinpath.kafka.connect.model;

import java.util.HashMap;
import java.util.Map;

/**
 * This class models the kafka-conect connector configuration
 * that is used when registering/updating Kafka Connect connectors.
 */
public class ConnectorConfiguration {

    /**
     * Connector name
     */
    private String name;

    /**
     * Connector configuration
     */
    private Map<String, String> config = new HashMap<>();


    public ConnectorConfiguration(String name, Map<String, String> config) {
        this.name = name;
        this.config = config;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public String toString() {
        return "ConnectorConfiguration{" +
                "name='" + name + '\'' +
                ", config=" + config +
                '}';
    }
}
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

import org.slf4j.Logger;
import org.testcontainers.containers.output.OutputFrame;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.function.Consumer;

final class Utils {
    public static final String CONFLUENT_PLATFORM_VERSION = "5.5.0";

    private Utils() {
    }

    /**
     * Retrieves a random port that is currently not in use on this machine.
     *
     * @return a free port
     */
    static int getRandomFreePort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Add logs consumer for the testcontainers.
     * Increase the log level when debugging exceptions that may occur at
     * the initialization of the containers.
     *
     * @param log
     * @return
     */
    public static Consumer<OutputFrame> containerLogsConsumer(Logger log) {
        return (OutputFrame outputFrame) -> log.trace(outputFrame.getUtf8String());
    }
}

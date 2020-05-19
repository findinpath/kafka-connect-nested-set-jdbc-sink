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

package com.findinpath.connect.nestedset.jdbc.sink.service;


import com.findinpath.connect.nestedset.jdbc.Utils;
import com.findinpath.connect.nestedset.jdbc.sink.jdbc.SinkNestedSetNodeLogOffsetRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Supplier;

public class SinkNestedSetNodeLogOffsetService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkNestedSetNodeLogOffsetService.class);

    private final Supplier<Connection> connectionSupplier;

    public SinkNestedSetNodeLogOffsetService(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    public Optional<Long> getNestedSetNodeLogOffset(String logTableName) {
        try (Connection connection = connectionSupplier.get()) {
            SinkNestedSetNodeLogOffsetRepository repository = new SinkNestedSetNodeLogOffsetRepository(connection);
            return repository.getNestedSetLogOffset(logTableName);
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return Optional.empty();
        }
    }

}

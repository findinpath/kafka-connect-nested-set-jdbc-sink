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

package com.findinpath.connect.nestedset.jdbc.sink.jdbc;

import com.findinpath.connect.nestedset.jdbc.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

public class SinkNestedSetNodeLogOffsetRepository {
    private static final String SELECT_LOG_OFFSET_SQL = "SELECT log_table_offset " +
            "WHERE log_table_name = ?";

    private static final Logger LOGGER = LoggerFactory.getLogger(SinkNestedSetNodeLogOffsetRepository.class);

    private final Connection connection;

    public SinkNestedSetNodeLogOffsetRepository(Connection connection) {
        this.connection = connection;
    }

    public Optional<Long> getNestedSetLogOffset(String logTableName) {
        LOGGER.info("Selecting the log_offset for name " + logTableName);

        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_LOG_OFFSET_SQL)) {

            pstmt.setString(1, logTableName);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return Optional.of(rs.getLong(1));
                }
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
        return Optional.empty();
    }
}
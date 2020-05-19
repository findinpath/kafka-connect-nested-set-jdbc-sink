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
import com.findinpath.connect.nestedset.jdbc.sink.model.SinkNestedSetNode;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

public class SinkNestedSetNodeRepository {

    private static final Calendar TZ_UTC = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private static final String SELECT_NESTED_SET_NODES_SQL =
            "SELECT id, label, lft, rgt, active, created, updated " +
                    "FROM nested_set_node ";
    private static final String SELECT_NESTED_SET_NODE_SQL =
            "SELECT id, label, lft, rgt, active, created, updated " +
                    "FROM nested_set_node " +
                    "WHERE id = ?";

    private static final String SELECT_IS_TABLE_EMPTY =
            "SELECT CASE \n" +
                    "         WHEN EXISTS (SELECT * FROM nested_set_node LIMIT 1) THEN 1\n" +
                    "         ELSE 0 \n" +
                    "       END";

    private final Connection connection;


    public SinkNestedSetNodeRepository(Connection connection) {
        this.connection = connection;
    }

    public List<SinkNestedSetNode> getNestedSetNodes() {
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_NESTED_SET_NODES_SQL);
             ResultSet rs = pstmt.executeQuery()) {
            List<SinkNestedSetNode> result = new ArrayList<>();
            while (rs.next()) {
                SinkNestedSetNode nestedSetNode = new SinkNestedSetNode();
                nestedSetNode.setId(rs.getLong(1));
                nestedSetNode.setLabel(rs.getString(2));
                nestedSetNode.setLeft(rs.getInt(3));
                nestedSetNode.setRight(rs.getInt(4));
                nestedSetNode.setActive(rs.getBoolean(5));
                nestedSetNode.setCreated(rs.getTimestamp(6, TZ_UTC).toInstant());
                nestedSetNode.setUpdated(rs.getTimestamp(7, TZ_UTC).toInstant());
                result.add(nestedSetNode);
            }

            return result;
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return null;
        }
    }

    public Optional<SinkNestedSetNode> getNestedSetNode(long id) {
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_NESTED_SET_NODE_SQL)) {

            pstmt.setLong(1, id);

            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    SinkNestedSetNode nestedSetNode = new SinkNestedSetNode();
                    nestedSetNode.setId(rs.getLong(1));
                    nestedSetNode.setLabel(rs.getString(2));
                    nestedSetNode.setLeft(rs.getInt(3));
                    nestedSetNode.setRight(rs.getInt(4));
                    nestedSetNode.setActive(rs.getBoolean(5));
                    nestedSetNode.setCreated(rs.getTimestamp(6, TZ_UTC).toInstant());
                    nestedSetNode.setUpdated(rs.getTimestamp(7, TZ_UTC).toInstant());
                    return Optional.of(nestedSetNode);
                }
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }

        return Optional.empty();
    }

    public boolean isTableEmpty() {
        try (PreparedStatement pstmt = connection.prepareStatement(SELECT_IS_TABLE_EMPTY);
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getInt(1) == 0;
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
        }
        return false;
    }
}
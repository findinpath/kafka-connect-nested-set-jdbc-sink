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

package com.findinpath.connect.nestedset.jdbc.source.service;


import com.findinpath.connect.nestedset.jdbc.Utils;
import com.findinpath.connect.nestedset.jdbc.source.jdbc.SourceNestedSetNodeRepository;
import com.findinpath.connect.nestedset.jdbc.source.model.SourceNestedSetNode;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class SourceNestedSetNodeService {

    private final Supplier<Connection> connectionSupplier;

    public SourceNestedSetNodeService(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    public List<SourceNestedSetNode> getNestedSetNodes() {
        try (Connection connection = connectionSupplier.get()) {
            SourceNestedSetNodeRepository nestedSetNodeRepository = new SourceNestedSetNodeRepository(connection);

            return nestedSetNodeRepository.getNestedSetNodes();
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return null;
        }
    }

    public Optional<SourceNestedSetNode> getNestedSetNode(long id) {
        try (Connection connection = connectionSupplier.get()) {
            SourceNestedSetNodeRepository nestedSetNodeRepository = new SourceNestedSetNodeRepository(connection);

            return nestedSetNodeRepository.getNestedSetNode(id);
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return null;
        }
    }

    public long insertNode(String data, long parentId) {
        try (Connection connection = connectionSupplier.get()) {
            connection.setAutoCommit(false);
            try {
                SourceNestedSetNodeRepository nestedSetNodeRepository = new SourceNestedSetNodeRepository(connection);
                SourceNestedSetNode parentNode = nestedSetNodeRepository.getNestedSetNode(parentId)
                        .orElseThrow(() -> new IllegalArgumentException("Invalid parent id " + parentId));

                nestedSetNodeRepository.makeSpaceForNewNode(parentNode.getRight());

                return nestedSetNodeRepository.insertNode(data, parentNode.getRight(), parentNode.getRight() + 1);
            } finally {
                connection.commit();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return 0;
        }
    }

    public long insertRootNode(String data) {
        try (Connection connection = connectionSupplier.get()) {
            try {
                connection.setAutoCommit(false);
                SourceNestedSetNodeRepository nestedSetNodeRepository = new SourceNestedSetNodeRepository(connection);

                boolean isTableEmpty = nestedSetNodeRepository.isTableEmpty();
                if (!isTableEmpty) {
                    throw new IllegalStateException("The nested_set table already contains data");
                }

                return nestedSetNodeRepository.insertNode(data, 1, 2);
            } finally {
                connection.commit();
                connection.setAutoCommit(true);
            }
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return 0;
        }
    }
}
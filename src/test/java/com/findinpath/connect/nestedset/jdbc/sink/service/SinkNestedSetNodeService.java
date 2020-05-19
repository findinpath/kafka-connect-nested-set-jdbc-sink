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
import com.findinpath.connect.nestedset.jdbc.sink.jdbc.SinkNestedSetNodeRepository;
import com.findinpath.connect.nestedset.jdbc.sink.model.SinkNestedSetNode;
import com.findinpath.connect.nestedset.jdbc.sink.tree.TreeBuilder;
import com.findinpath.connect.nestedset.jdbc.sink.tree.TreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class SinkNestedSetNodeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(SinkNestedSetNodeService.class);

    private final Supplier<Connection> connectionSupplier;

    public SinkNestedSetNodeService(Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
    }

    public Optional<TreeNode<SinkNestedSetNode>> getTree() {
        LOGGER.info("Building the tree from the persistence");

        try (Connection connection = connectionSupplier.get()) {
            SinkNestedSetNodeRepository nestedSetNodeRepository = new SinkNestedSetNodeRepository(connection);
            List<SinkNestedSetNode> nestedSetNodes = nestedSetNodeRepository.getNestedSetNodes();
            if (nestedSetNodes.isEmpty()) {
                return Optional.empty();
            }

            return TreeBuilder.buildTree(nestedSetNodes);
        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return Optional.empty();
        }
    }

    public Optional<SinkNestedSetNode> getNestedSetNode(long nodeId) {
        try (Connection connection = connectionSupplier.get()) {
            SinkNestedSetNodeRepository nestedSetNodeRepository = new SinkNestedSetNodeRepository(connection);
            return nestedSetNodeRepository.getNestedSetNode(nodeId);

        } catch (SQLException e) {
            Utils.sneakyThrow(e);
            return Optional.empty();
        }
    }

}

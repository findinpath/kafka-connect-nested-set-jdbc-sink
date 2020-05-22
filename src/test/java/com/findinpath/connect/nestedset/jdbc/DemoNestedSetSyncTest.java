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

package com.findinpath.connect.nestedset.jdbc;


import com.findinpath.connect.nestedset.jdbc.sink.model.SinkNestedSetNode;
import com.findinpath.connect.nestedset.jdbc.sink.service.SinkNestedSetNodeLogOffsetService;
import com.findinpath.connect.nestedset.jdbc.sink.service.SinkNestedSetNodeService;
import com.findinpath.connect.nestedset.jdbc.sink.tree.TreeNode;
import com.findinpath.connect.nestedset.jdbc.source.model.SourceNestedSetNode;
import com.findinpath.connect.nestedset.jdbc.source.service.SourceNestedSetNodeService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * This class contains a series of end-to-end tests verifying
 * that the sync of a nested set model via kafka-connect-jdbc
 * works as expected.
 * <p>
 * The tests involve the following preparation steps:
 * <ul>
 *     <li>bootstrapping testcontainers for:
 *      <ul>
 *          <li>PostgreSQL source and sink database</li>
 *          <li>Apache Kafka ecosystem</li>
 *      </ul>
 *     </li>
 *     <li>registering a kafka-connect-jdbc connector for the source database table containing the nested set model</li>
 *     <li>registering a kafka-connect-jdbc connector for the sink database table where to synchronize the nested set model</li>
 * </ul>
 * <p>
 * The end-to-end tests make sure that the contents of the nested set model
 * are synced from source to sink even independently whether the tree suffers small or bigger changes.
 * NOTE that adding a node to the nested set model involves changing the left and right coordinates
 * of a big portion of the nested set.
 *
 * @see {@link AbstractNestedSetSyncTest}
 */
public class DemoNestedSetSyncTest extends AbstractNestedSetSyncTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(DemoNestedSetSyncTest.class);

    private SinkNestedSetNodeService sinkNestedSetNodeService;
    private SinkNestedSetNodeLogOffsetService sinkNestedSetNodeLogOffsetService;

    private SourceNestedSetNodeService sourceNestedSetNodeService;

    private static void print(TreeNode<SinkNestedSetNode> treeNode, StringBuilder buffer, String prefix, String childrenPrefix) {
        buffer.append(prefix);
        buffer.append("|" + treeNode.getNestedSetNode().getLeft() + "| " + treeNode.getNestedSetNode().getLabel() + " |" + treeNode.getNestedSetNode().getRight() + "|");
        buffer.append('\n');
        if (treeNode.getChildren() != null && !treeNode.getChildren().isEmpty()) {
            int nodeLeftDigitsCount = Integer.toString(treeNode.getNestedSetNode().getLeft()).length();
            String leftPad = String.format("%1$" + (nodeLeftDigitsCount + 3) + "s", "");
            for (Iterator<TreeNode<SinkNestedSetNode>> it = treeNode.getChildren().iterator(); it.hasNext(); ) {
                TreeNode<SinkNestedSetNode> next = it.next();
                if (it.hasNext()) {
                    print(next, buffer,
                            childrenPrefix + leftPad + "├── ",
                            childrenPrefix + leftPad + "│   ");
                } else {
                    print(next, buffer,
                            childrenPrefix + leftPad + "└── ",
                            childrenPrefix + leftPad + "    ");
                }
            }
        }
    }

    private static boolean assertEquality(SourceNestedSetNode sourceNestedSetNode,
                                          SinkNestedSetNode sinkNestedSetNode) {
        return Objects.equals(sourceNestedSetNode.getId(), sinkNestedSetNode.getId())
                && Objects.equals(sourceNestedSetNode.getLabel(), sinkNestedSetNode.getLabel())
                && Objects.equals(sourceNestedSetNode.getLeft(), sinkNestedSetNode.getLeft())
                && Objects.equals(sourceNestedSetNode.getRight(), sinkNestedSetNode.getRight())
                && Objects.equals(sourceNestedSetNode.getCreated(), sinkNestedSetNode.getCreated())
                && Objects.equals(sourceNestedSetNode.getUpdated(), sinkNestedSetNode.getUpdated());
    }

    @BeforeEach
    public void setup() throws Exception {
        super.setup();

        sourceNestedSetNodeService = new SourceNestedSetNodeService(sourceConnectionProvider);

        sinkNestedSetNodeService = new SinkNestedSetNodeService(sinkConnectionProvider);
        sinkNestedSetNodeLogOffsetService = new SinkNestedSetNodeLogOffsetService(sinkConnectionProvider);
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }

    /**
     * This test ensures the sync accuracy for the following simple tree:
     *
     * <pre>
     * |1| A |6|
     *     ├── |2| B |3|
     *     └── |4| C |5|
     * </pre>
     */
    @Test
    public void simpleTreeDemo() {

        // Create a nested set in the persistence of the source database
        // with the following configuration:
        //  left=1  label=A   right=6
        //            left = 2  label=B  right = 3
        //            left = 4  label=C  right = 5

        String aNodeLabel = "A";
        long aNodeId = sourceNestedSetNodeService.insertRootNode(aNodeLabel);
        String bNodeLabel = "B";
        long bNodeId = sourceNestedSetNodeService.insertNode(bNodeLabel, aNodeId);
        String cNodeLabel = "C";
        long cNodeId = sourceNestedSetNodeService.insertNode(cNodeLabel, aNodeId);

        awaitForTheSyncOfTheNode(cNodeId);
        logSinkTreeContent();
    }

    /**
     * Accuracy test for the sync of the contents of the following nested set model:
     *
     * <pre>
     * |1| Food |18|
     *     ├── |2| Fruit |11|
     *     │       ├── |3| Red |6|
     *     │       │       └── |4| Cherry |5|
     *     │       └── |7| Yellow |10|
     *     │               └── |8| Banana |9|
     *     └── |12| Meat |17|
     *              ├── |13| Beef |14|
     *              └── |15| Pork |16|
     * </pre>
     */
    @Test
    public void foodTreeSyncDemo() {
        long foodNodeId = sourceNestedSetNodeService.insertRootNode("Food");
        long fruitNodeId = sourceNestedSetNodeService.insertNode("Fruit", foodNodeId);
        long redFruitNodeId = sourceNestedSetNodeService.insertNode("Red", fruitNodeId);
        sourceNestedSetNodeService.insertNode("Cherry", redFruitNodeId);
        long yellowFruitNodeId = sourceNestedSetNodeService.insertNode("Yellow", fruitNodeId);
        sourceNestedSetNodeService.insertNode("Banana", yellowFruitNodeId);
        long meatNodeId = sourceNestedSetNodeService.insertNode("Meat", foodNodeId);
        sourceNestedSetNodeService.insertNode("Beef", meatNodeId);
        sourceNestedSetNodeService.insertNode("Pork", meatNodeId);

        awaitForTheSyncOfTheNode(foodNodeId);
        logSinkTreeContent();
    }

    /**
     * This test makes sure that the syncing between source and sink
     * works when successively over the time new nodes are added to the nested
     * set model.
     * <p>
     * At the end of the test the synced nested set model should look like:
     *
     * <pre>
     *     |1| Clothing |14|
     *     ├── |2| Men's |5|
     *     │       └── |3| Suits |4|
     *     └── |6| Women's |13|
     *             ├── |7| Dresses |8|
     *             ├── |9| Skirts |10|
     *             └── |11| Blouses |12|
     * </pre>
     */
    @Test
    public void syncingSuccessiveChangesToTheTreeDemo() {

        // Create initially a nested set with only the root node
        //  left=1  label=Clothing   right=2

        long clothingNodeId = sourceNestedSetNodeService.insertRootNode("Clothing");
        awaitForTheSyncOfTheNode(clothingNodeId);
        logSinkTreeContent();

        // Add now Men's and Women's children and wait for the syncing
        long mensNodeId = sourceNestedSetNodeService.insertNode("Men's", clothingNodeId);
        long womensNodeId = sourceNestedSetNodeService.insertNode("Women's", clothingNodeId);

        awaitForTheSyncOfTheNode(womensNodeId);
        logSinkTreeContent();

        // Add new children categories
        sourceNestedSetNodeService.insertNode("Suits", mensNodeId);
        sourceNestedSetNodeService.insertNode("Dresses", womensNodeId);
        sourceNestedSetNodeService.insertNode("Skirts", womensNodeId);
        sourceNestedSetNodeService.insertNode("Blouses", womensNodeId);

        awaitForTheSyncOfTheNode(womensNodeId);
        logSinkTreeContent();
    }

    private void awaitForTheSyncOfTheNode(long nodeId) {
        SourceNestedSetNode sourceNestedSetNode = sourceNestedSetNodeService.getNestedSetNode(nodeId)
                .orElseThrow(() -> new IllegalStateException("Node with ID " + nodeId + " was not found"));

        // and now  verify that the syncing via kafka-connect-jdbc
        // between source and sink database works
        await().atMost(20, TimeUnit.SECONDS)
                .pollInterval(250, TimeUnit.MILLISECONDS)
                .until(() -> {
                    Optional<SinkNestedSetNode> sinkNestedSetNode = sinkNestedSetNodeService.getNestedSetNode(nodeId);
                    if (sinkNestedSetNode.isPresent()) {
                        return assertEquality(sourceNestedSetNode, sinkNestedSetNode.get());
                    }
                    return false;
                });
    }

    private void logSinkTreeContent() {
        TreeNode<SinkNestedSetNode> sinkNestedSetRootNode = sinkNestedSetNodeService.getTree()
                .orElseThrow(() -> new IllegalStateException("Sink tree hasn't been initialized"));

        StringBuilder sinkTreeRepresentation = new StringBuilder();
        print(sinkNestedSetRootNode, sinkTreeRepresentation, "", "");
        LOGGER.info("Sink nested set node current configuration: \n" + sinkTreeRepresentation.toString());
    }
}

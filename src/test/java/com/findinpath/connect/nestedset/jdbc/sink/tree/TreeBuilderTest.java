package com.findinpath.connect.nestedset.jdbc.sink.tree;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

import static com.findinpath.connect.nestedset.jdbc.sink.tree.TreeBuilder.buildTree;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TreeBuilderTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(TreeBuilderTest.class);

    private static void print(TreeNode<SimpleNestedSetNode> treeNode, StringBuilder buffer, String prefix, String childrenPrefix) {
        buffer.append(prefix);
        buffer.append("|" + treeNode.getNestedSetNode().getLeft() + "|  |" + treeNode.getNestedSetNode().getRight() + "|");
        buffer.append('\n');
        if (treeNode.getChildren() != null && !treeNode.getChildren().isEmpty()) {
            int nodeLeftDigitsCount = Integer.toString(treeNode.getNestedSetNode().getLeft()).length();
            String leftPad = String.format("%1$" + (nodeLeftDigitsCount + 3) + "s", "");
            for (Iterator<TreeNode<SimpleNestedSetNode>> it = treeNode.getChildren().iterator(); it.hasNext(); ) {
                TreeNode<SimpleNestedSetNode> next = it.next();
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

    @Test
    public void treeWithOneNodeAccuracy() {
        SimpleNestedSetNode rootNestedSetNode = new SimpleNestedSetNode(1, 2);
        Optional<TreeNode<SimpleNestedSetNode>> root = buildTree(Collections.singletonList(rootNestedSetNode));
        assertTrue(root.isPresent());
        logTreeContent(root.get());
        assertThat(root.get().getNestedSetNode(), equalTo(rootNestedSetNode));
        assertThat(root.get().getChildren(), anyOf(is(nullValue()), hasSize(0)));
    }

    @Test
    @DisplayName("The right coordinate of the root node is greater than number_of_nodes * 2")
    public void treeWithOneNodeRightTooBigFailure() {
        SimpleNestedSetNode rootNestedSetNode = new SimpleNestedSetNode(1, 4);
        Optional<TreeNode<SimpleNestedSetNode>> root = buildTree(Collections.singletonList(rootNestedSetNode));
        assertFalse(root.isPresent());
    }

    @Test
    @DisplayName("The right coordinate of the nested set node must be greater than the left coordinate")
    public void treeWithOneNodeLeftBiggerThanRightFailure() {
        SimpleNestedSetNode rootNestedSetNode = new SimpleNestedSetNode(2, 1);
        Optional<TreeNode<SimpleNestedSetNode>> root = buildTree(Collections.singletonList(rootNestedSetNode));
        assertFalse(root.isPresent());
    }

    @Test
    public void treeWithTwoNodesAccuracy() {
        SimpleNestedSetNode rootNestedSetNode = new SimpleNestedSetNode(1, 4);
        SimpleNestedSetNode childNestedSetNode = new SimpleNestedSetNode(2, 3);
        Optional<TreeNode<SimpleNestedSetNode>> root = buildTree(asList(childNestedSetNode, rootNestedSetNode));
        assertTrue(root.isPresent());
        logTreeContent(root.get());
        TreeNode<SimpleNestedSetNode> expectedChild = new TreeNode<>(childNestedSetNode);
        TreeNode<SimpleNestedSetNode> expectedRoot = new TreeNode<>(rootNestedSetNode, asList(expectedChild));
        assertThat(root.get(), equalTo(expectedRoot));
    }

    @Test
    public void treeWithMultipleChildrenAccuracy() {
        SimpleNestedSetNode rootNestedSetNode = new SimpleNestedSetNode(1, 6);
        SimpleNestedSetNode child1NestedSetNode = new SimpleNestedSetNode(2, 3);
        SimpleNestedSetNode child2NestedSetNode = new SimpleNestedSetNode(4, 5);
        Optional<TreeNode<SimpleNestedSetNode>> root = buildTree(asList(child1NestedSetNode, rootNestedSetNode, child2NestedSetNode));
        assertTrue(root.isPresent());
        TreeNode<SimpleNestedSetNode> expectedChild1 = new TreeNode<>(child1NestedSetNode);
        TreeNode<SimpleNestedSetNode> expectedChild2 = new TreeNode<>(child2NestedSetNode);
        TreeNode<SimpleNestedSetNode> expectedRoot = new TreeNode<>(rootNestedSetNode, asList(expectedChild1, expectedChild2));
        assertThat(root.get(), equalTo(expectedRoot));
    }

    /**
     * Test the accuracy for modelling the following tree
     * <pre>
     * |1| Food |12|
     *     ├── |2| Fruit |7|
     *     │       ├── |3| Apple |4|
     *     │       └── |5| Banana |6|
     *     └── |8| Meat |11|
     *              ├── |9| Beef |10|
     * </pre>
     */
    @Test
    public void treeWithMultipleLevelsAccuracy() {
        SimpleNestedSetNode foodNestedSetNode = new SimpleNestedSetNode(1, 12);
        SimpleNestedSetNode fruitNestedSetNode = new SimpleNestedSetNode(2, 7);
        SimpleNestedSetNode appleNestedSetNode = new SimpleNestedSetNode(3, 4);
        SimpleNestedSetNode bananaNestedSetNode = new SimpleNestedSetNode(5, 6);
        SimpleNestedSetNode meatNestedSetNode = new SimpleNestedSetNode(8, 11);
        SimpleNestedSetNode beefNestedSetNode = new SimpleNestedSetNode(9, 10);
        Optional<TreeNode<SimpleNestedSetNode>> foodTreeNode = buildTree(asList(
                foodNestedSetNode,
                fruitNestedSetNode, appleNestedSetNode, bananaNestedSetNode,
                meatNestedSetNode, beefNestedSetNode
        ));
        assertTrue(foodTreeNode.isPresent());
        logTreeContent(foodTreeNode.get());
        TreeNode<SimpleNestedSetNode> expectedAppleTreeNode = new TreeNode<>(appleNestedSetNode);
        TreeNode<SimpleNestedSetNode> expectedBananaTreeNode = new TreeNode<>(bananaNestedSetNode);
        TreeNode<SimpleNestedSetNode> expectedFruitTreeNode = new TreeNode<>(fruitNestedSetNode, asList(expectedAppleTreeNode, expectedBananaTreeNode));
        TreeNode<SimpleNestedSetNode> expectedBeefTreeNode = new TreeNode<>(beefNestedSetNode);
        TreeNode<SimpleNestedSetNode> expectedMeatTreeNode = new TreeNode<>(meatNestedSetNode, asList(expectedBeefTreeNode));
        TreeNode<SimpleNestedSetNode> expectedFoodTreeNode = new TreeNode<>(foodNestedSetNode, asList(expectedFruitTreeNode, expectedMeatTreeNode));
        assertThat(foodTreeNode.get(), equalTo(expectedFoodTreeNode));
    }

    @Test
    @DisplayName("Root node doesn't have the greatest right coordinate")
    public void treeWithMultipleChildrenInvalidChildCoordinatesFailure() {
        SimpleNestedSetNode rootNestedSetNode = new SimpleNestedSetNode(1, 5);
        SimpleNestedSetNode child1NestedSetNode = new SimpleNestedSetNode(2, 6);
        SimpleNestedSetNode child2NestedSetNode = new SimpleNestedSetNode(3, 4);
        Optional<TreeNode<SimpleNestedSetNode>> root = buildTree(asList(child1NestedSetNode, rootNestedSetNode, child2NestedSetNode));
        assertFalse(root.isPresent());
    }

    @Test
    @DisplayName("Duplicate coordinate")
    public void treeWithMultipleChildrenDuplicateCoordinateFailure() {
        SimpleNestedSetNode rootNestedSetNode = new SimpleNestedSetNode(1, 6);
        SimpleNestedSetNode child1NestedSetNode = new SimpleNestedSetNode(2, 5);
        SimpleNestedSetNode child2NestedSetNode = new SimpleNestedSetNode(4, 5);
        Optional<TreeNode<SimpleNestedSetNode>> root = buildTree(asList(child1NestedSetNode, rootNestedSetNode, child2NestedSetNode));
        assertFalse(root.isPresent());
    }

    private void logTreeContent(TreeNode<SimpleNestedSetNode> treeNode) {
        StringBuilder sinkTreeRepresentation = new StringBuilder();
        print(treeNode, sinkTreeRepresentation, "", "");
        LOGGER.info("Tree representation: \n" + sinkTreeRepresentation.toString());
    }

    private static class SimpleNestedSetNode implements NestedSetNode {
        private final int left;
        private final int right;

        public SimpleNestedSetNode(int left, int right) {
            this.left = left;
            this.right = right;
        }

        @Override
        public int getLeft() {
            return left;
        }

        @Override
        public int getRight() {
            return right;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SimpleNestedSetNode that = (SimpleNestedSetNode) o;
            return left == that.left &&
                    right == that.right;
        }

        @Override
        public int hashCode() {
            return Objects.hash(left, right);
        }
    }
}

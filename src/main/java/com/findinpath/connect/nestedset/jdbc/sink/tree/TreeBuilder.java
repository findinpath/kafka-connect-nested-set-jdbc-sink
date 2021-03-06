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

package com.findinpath.connect.nestedset.jdbc.sink.tree;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility class which performs the validation of the provided list of
 * nested set nodes by trying to build the corresponding tree structure
 * out of them.
 */
public final class TreeBuilder {
    private TreeBuilder() {
    }

    public static <T extends NestedSetNode> Optional<TreeNode<T>> buildTree(List<T> nestedSetNodes) {
        if (!isValidNestedSet(nestedSetNodes)) return Optional.empty();

        Iterator<T> nestedSetNodeIterator = nestedSetNodes
                .stream()
                .sorted(Comparator.comparing(NestedSetNode::getLeft))
                .iterator();
        T rootNestedSetNode = nestedSetNodeIterator.next();
        TreeNode<T> root = new TreeNode<>(rootNestedSetNode);
        Stack<TreeNode<T>> stack = new Stack<>();
        stack.push(root);
        while (nestedSetNodeIterator.hasNext()) {
            T nestedSetNode = nestedSetNodeIterator.next();

            if (stack.isEmpty()) return Optional.empty();
            // find the corresponding parent node
            while (stack.peek().getNestedSetNode().getRight() < nestedSetNode.getRight()) {
                stack.pop();
                if (stack.isEmpty()) return Optional.empty();
            }
            TreeNode<T> parent = stack.peek();

            TreeNode<T> child = parent.addChild(nestedSetNode);
            stack.push(child);
        }
        return Optional.of(root);
    }

    private static <T extends NestedSetNode> boolean isValidNestedSet(List<T> nestedSetNodes) {
        if (nestedSetNodes == null || nestedSetNodes.isEmpty()) return false;

        Comparator<Integer> naturalOrdering = Integer::compareTo;
        Optional<T> nestedSetNodeWithInvalidCoordinates = nestedSetNodes.stream()
                .filter(nestedSetNode -> !isValid(nestedSetNode))
                .findAny();
        if (nestedSetNodeWithInvalidCoordinates.isPresent()) {
            return false;
        }

        List<Integer> leftCoordinatesSorted = nestedSetNodes
                .stream()
                .sorted(Comparator.comparing(NestedSetNode::getLeft))
                .map(NestedSetNode::getLeft)
                .collect(Collectors.toList());
        // preordered representation of the nested set should be strictly ordered
        if (!isInStrictOrder(leftCoordinatesSorted, naturalOrdering)) {
            return false;
        }

        List<Integer> rightCoordinatesSorted = nestedSetNodes
                .stream()
                .sorted(Comparator.comparing(NestedSetNode::getRight).reversed())
                .map(NestedSetNode::getRight)
                .collect(Collectors.toList());
        // postordered representation of the nested set should be strictly ordered
        if (!isInStrictOrder(rightCoordinatesSorted, naturalOrdering.reversed())) {
            return false;
        }
        List<Integer> allCoordinates = Stream.of(leftCoordinatesSorted, rightCoordinatesSorted)
                .flatMap(Collection::stream)
                .sorted()
                .collect(Collectors.toList());
        // verify that there are no duplicated coordinates in the nested set
        if (!isInStrictOrder(allCoordinates, naturalOrdering)) {
            return false;
        }

        // the maximum value for a coordinate must correspond to the double of the number of nodes
        return allCoordinates.get(allCoordinates.size() - 1) == nestedSetNodes.size() * 2;
    }


    private static <T extends NestedSetNode> boolean isValid(T nestedSetNode) {
        return nestedSetNode.getLeft() < nestedSetNode.getRight();
    }

    private static <T> boolean isInStrictOrder(
            Iterable<? extends T> iterable, Comparator<T> comparator) {
        Iterator<? extends T> it = iterable.iterator();
        if (it.hasNext()) {
            T prev = it.next();
            while (it.hasNext()) {
                T next = it.next();
                if (comparator.compare(prev, next) >= 0) {
                    return false;
                }
                prev = next;
            }
        }
        return true;
    }
}

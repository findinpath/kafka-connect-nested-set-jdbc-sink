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

    public static Optional<TreeNode> buildTree(List<NestedSetNode> nestedSetNodes) {
        if (!isValidNestedSet(nestedSetNodes)) return Optional.empty();

        Iterator<NestedSetNode> nestedSetNodeIterator = nestedSetNodes
                .stream()
                .sorted(Comparator.comparing(NestedSetNode::getLeft))
                .iterator();
        NestedSetNode rootNestedSetNode = nestedSetNodeIterator.next();
        TreeNode root = new TreeNode(rootNestedSetNode);
        Stack<TreeNode> stack = new Stack<>();
        stack.push(root);
        while (nestedSetNodeIterator.hasNext()) {
            NestedSetNode nestedSetNode = nestedSetNodeIterator.next();

            if (stack.isEmpty()) return Optional.empty();
            // find the corresponding parent node
            while (stack.peek().getNestedSetNode().getRight() < nestedSetNode.getRight()) {
                stack.pop();
            }
            if (stack.isEmpty()) return Optional.empty();
            TreeNode parent = stack.peek();

            TreeNode child = parent.addChild(nestedSetNode);
            stack.push(child);
        }
        return Optional.of(root);
    }

    private static boolean isValidNestedSet(List<NestedSetNode> nestedSetNodes) {
        if (nestedSetNodes == null || nestedSetNodes.isEmpty()) return false;

        Comparator<Comparable> naturalOrdering = Comparable::compareTo;
        Optional<NestedSetNode> nestedSetNodeWithInvalidCoordinates = nestedSetNodes.stream()
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


    private static boolean isValid(NestedSetNode nestedSetNode) {
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

package com.findinpath.connect.nestedset.jdbc.sink.tree;

import java.util.ArrayList;
import java.util.List;

/**
 * Specific tree node structure containing nested set nodes.
 *
 * @see {@link TreeBuilder#buildTree(List)}
 */
public class TreeNode {
    private NestedSetNode nestedSetNode;
    private List<TreeNode> children;


    public TreeNode(NestedSetNode nestedSetNode) {
        this.nestedSetNode = nestedSetNode;
    }

    public TreeNode(NestedSetNode nestedSetNode, List<TreeNode> children) {
        this(nestedSetNode);
        this.children = children == null ? null : new ArrayList<>(children);
    }

    public NestedSetNode getNestedSetNode() {
        return nestedSetNode;
    }

    public List<TreeNode> getChildren() {
        return children;
    }


    public TreeNode addChild(NestedSetNode nestedSetNode) {
        if (children == null) {
            children = new ArrayList<>();
        }
        TreeNode child = new TreeNode(nestedSetNode);
        children.add(child);
        return child;
    }
}

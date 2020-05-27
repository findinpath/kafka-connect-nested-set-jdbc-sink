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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Specific tree node structure containing nested set nodes.
 *
 * @see TreeBuilder#buildTree(List)
 */
public class TreeNode<T extends NestedSetNode> {
    private T nestedSetNode;
    private List<TreeNode<T>> children;


    public TreeNode(T nestedSetNode) {
        this.nestedSetNode = nestedSetNode;
    }

    public TreeNode(T nestedSetNode, List<TreeNode<T>> children) {
        this(nestedSetNode);
        this.children = children == null ? null : new ArrayList<>(children);
    }

    public T getNestedSetNode() {
        return nestedSetNode;
    }

    public List<TreeNode<T>> getChildren() {
        return children;
    }


    public TreeNode<T> addChild(T nestedSetNode) {
        if (children == null) {
            children = new ArrayList<>();
        }
        TreeNode<T> child = new TreeNode(nestedSetNode);
        children.add(child);
        return child;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TreeNode<?> treeNode = (TreeNode<?>) o;
        return Objects.equals(nestedSetNode, treeNode.nestedSetNode) &&
                Objects.equals(children, treeNode.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nestedSetNode, children);
    }
}

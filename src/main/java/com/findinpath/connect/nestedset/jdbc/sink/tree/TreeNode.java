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

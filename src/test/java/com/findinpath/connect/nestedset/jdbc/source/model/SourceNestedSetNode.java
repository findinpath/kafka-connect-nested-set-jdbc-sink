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

package com.findinpath.connect.nestedset.jdbc.source.model;

import com.findinpath.connect.nestedset.jdbc.sink.tree.NestedSetNode;

import java.time.Instant;

/**
 * Models a nested set node.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Nested_set_model">Nested Set Model</a>
 */
public class SourceNestedSetNode implements NestedSetNode {
    private long id;
    private String label;

    private int left;
    private int right;
    private boolean active;
    private Instant created;
    private Instant updated;

    public SourceNestedSetNode() {
    }

    public SourceNestedSetNode(int id, String label, int left, int right) {
        this.id = id;
        this.label = label;
        this.left = left;
        this.right = right;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    @Override
    public int getLeft() {
        return left;
    }

    public void setLeft(int left) {
        this.left = left;
    }

    @Override
    public int getRight() {
        return right;
    }

    public void setRight(int right) {
        this.right = right;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public Instant getCreated() {
        return created;
    }

    public void setCreated(Instant created) {
        this.created = created;
    }

    public Instant getUpdated() {
        return updated;
    }

    public void setUpdated(Instant updated) {
        this.updated = updated;
    }

    @Override
    public String toString() {
        return "NestedSetNode{" +
                "id=" + id +
                ", label=" + label +
                ", left=" + left +
                ", right=" + right +
                ", active=" + active +
                ", created=" + created +
                ", updated=" + updated +
                '}';
    }
}
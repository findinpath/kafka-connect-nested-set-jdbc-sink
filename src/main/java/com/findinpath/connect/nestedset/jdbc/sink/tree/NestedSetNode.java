package com.findinpath.connect.nestedset.jdbc.sink.tree;

/**
 * Basic interface used for modelling nested set nodes.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Nested_set_model">Nested Set Model</a>
 */
public interface NestedSetNode {
    int getLeft();

    int getRight();
}

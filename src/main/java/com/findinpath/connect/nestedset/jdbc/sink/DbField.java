package com.findinpath.connect.nestedset.jdbc.sink;

import org.apache.kafka.connect.data.Schema;

/**
 *
 * @see PreparedStatementBinder#bindField(int, org.apache.kafka.connect.data.Schema, java.lang.Object)
 */
public class DbField {
    private final String name;
    private final Schema schema;
    private final Object value;

    public DbField(String name, Schema schema, Object value) {
        this.name = name;
        this.schema = schema;
        this.value = value;
    }
}

package com.findinpath.connect.nestedset.jdbc;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
public class DemoNestedSetSyncTest extends  AbstractNestedSetSyncTest {

    @BeforeEach
    public void setup() throws Exception{
        super.setup();
    }

    @AfterEach
    public void tearDown() {
        super.tearDown();
    }


    @Test
    public void simpleTreeDemo(){}
}

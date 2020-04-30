package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.dialect.DatabaseDialect;
import com.findinpath.connect.nestedset.jdbc.util.CachedConnectionProvider;
import com.findinpath.connect.nestedset.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class NestedSetSynchronizer {
    private static final Logger log = LoggerFactory
            .getLogger(NestedSetSynchronizer.class);

    private final CachedConnectionProvider cachedConnectionProvider;

    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final TableId tableId;
    private final TableId logTableId;
    private final TableId logOffsetTableId;

    public NestedSetSynchronizer(
            JdbcSinkConfig config,
            DatabaseDialect dbDialect,
            DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;

        this.tableId = dbDialect.parseTableIdentifier(config.tableName);
        this.logTableId = dbDialect.parseTableIdentifier(config.logTableName);
        this.logOffsetTableId = dbDialect.parseTableIdentifier(config.logOffsetTableName);

        this.cachedConnectionProvider = new CachedConnectionProvider(this.dbDialect) {
            @Override
            protected void onConnect(Connection connection) throws SQLException {
                log.info("NestedSetSynchronizer Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    public void synchronize() throws SQLException {
        final Connection connection = cachedConnectionProvider.getConnection();

        dbStructure.createLogOffsetTableIfNecessary(
                config,
                connection,
                logOffsetTableId);

       // ...
        connection.commit();
    }
}

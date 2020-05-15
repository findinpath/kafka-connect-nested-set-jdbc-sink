package com.findinpath.connect.nestedset.jdbc.sink;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcHelper {
    public interface ResultSetReadCallback {
        void read(final ResultSet rs) throws SQLException;
    }

    private final Connection connection;

    public JdbcHelper(String jdbcUrl, String username, String password) throws SQLException {
        Properties connectionProps = new Properties();
        connectionProps.put("user", username);
        connectionProps.put("password", password);
        connection = DriverManager.getConnection(jdbcUrl, connectionProps);
    }

    public int select(final String query, final JdbcHelper.ResultSetReadCallback callback) throws SQLException {
        int count = 0;
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    callback.read(rs);
                    count++;
                }
            }
        }
        return count;
    }

    public void execute(String sql) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(sql);
            connection.commit();
        }
    }
}

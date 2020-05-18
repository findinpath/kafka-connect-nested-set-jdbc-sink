package com.findinpath.connect.nestedset.jdbc.sink;

import com.findinpath.connect.nestedset.jdbc.testcontainers.OracleContainer;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.SQLException;

@Disabled
@Testcontainers
public class OracleJdbcDbWriterTest extends JdbcDbWriterTest {

    @Container
    protected static OracleContainer oracleContainer = new OracleContainer("oracle/database:18.4.0-xe")
            .withInitScript("sink/init_jdbcdbdriver_oracle.sql");

    @Override
    protected String getJdbcUrl() {
        return oracleContainer.getJdbcUrl();
    }

    @Override
    protected String getJdbcUsername() {
        return oracleContainer.getUsername();
    }

    @Override
    protected String getJdbcPassword() {
        return oracleContainer.getPassword();
    }

    @Override
    protected void dropTableIfExists(String tableName) throws SQLException {
        try {
            jdbcHelper.execute("DROP TABLE " + tableName);
        }catch(SQLException e){
            // swallow eventual exceptions when the table doesn't exists
        }
    }
}

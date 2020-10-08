package com.nosqldriver.jdbc.http;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class PreparedStatementControllerTest extends StatementControllerTest {
    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.prepareStatement("select 1");
    }
}

package com.nosqldriver.jdbc.http;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class PreparedStatementControllerTest extends StatementControllerTest {
    @Override
    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.prepareStatement("select 1");
    }

    @Override
    protected ResultSet executeQuery(Connection conn, String query) throws SQLException {
        return conn.prepareStatement(query).executeQuery();
    }
}

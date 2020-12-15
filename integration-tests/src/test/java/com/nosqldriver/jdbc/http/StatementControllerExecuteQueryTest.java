package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementControllerExecuteQueryTest extends StatementControllerTestBase<Statement, Integer> {
    @SafeVarargs
    @Override
    protected final ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<Statement, SQLException>... setters) throws SQLException {
        Statement statement = conn.createStatement();
        runSetters(statement, setters);
        return statement.executeQuery(query);
    }

    @Override
    protected Integer executeUpdate(Connection conn, String update) throws SQLException {
        return conn.createStatement().executeUpdate(update);
    }
}

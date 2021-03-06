package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatementControllerExecuteTest extends StatementControllerTestBase<Statement, Integer> {
    @SafeVarargs
    @Override
    protected final ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<Statement, SQLException>... setters) throws SQLException {
        Statement statement = conn.createStatement();
        runSetters(statement, setters);
        assertTrue(statement.execute(query));
        return statement.getResultSet();
    }

    @Override
    protected Integer executeUpdate(Connection conn, String update) throws SQLException {
        return conn.createStatement().executeUpdate(update);
    }

    public static class StatementControllerExecuteLargeTest extends StatementControllerTestBase<Statement, Long> {
        @SafeVarargs
        @Override
        protected final ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<Statement, SQLException>... setters) throws SQLException {
            Statement statement = conn.createStatement();
            runSetters(statement, setters);
            assertTrue(statement.execute(query));
            return statement.getResultSet();
        }

        @Override
        protected Long executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeLargeUpdate(update);
        }
    }
}

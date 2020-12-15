package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class StatementControllerExecute2Test extends StatementControllerTestBase<Statement> {
    @SafeVarargs
    @Override
    protected final ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<Statement, SQLException>... setters) throws SQLException {
        Statement statement = conn.createStatement();
        runSetters(statement, setters);
        assertTrue(execute(query, statement));
        return statement.getResultSet();
    }

    protected abstract boolean execute(String query, Statement statement) throws SQLException;

    @Override
    protected int executeUpdate(Connection conn, String update) throws SQLException {
        return conn.createStatement().executeUpdate(update);
    }

    public static class StatementControllerExecuteReturnGeneratedKeysTest extends StatementControllerExecute2Test {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, Statement.RETURN_GENERATED_KEYS);
        }

        @Override
        protected int executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update, Statement.RETURN_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteNoGeneratedKeysTest extends StatementControllerExecute2Test {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, Statement.NO_GENERATED_KEYS);
        }

        @Override
        protected int executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update, Statement.NO_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteEmptyColumnIndexesTest extends StatementControllerExecute2Test {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new int[0]);
        }

        @Override
        protected int executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update, new int[0]);
        }
    }

    public static class StatementControllerExecuteEmptyColumnKeysTest extends StatementControllerExecute2Test {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new String[0]);
        }

        @Override
        protected int executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update, new String[0]);
        }
    }
}

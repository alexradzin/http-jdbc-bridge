package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class StatementControllerExecute2Test<R extends Number> extends StatementControllerTestBase<Statement, R> {
    @SafeVarargs
    @Override
    protected final ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<Statement, SQLException>... setters) throws SQLException {
        Statement statement = conn.createStatement();
        runSetters(statement, setters);
        assertTrue(execute(query, statement));
        return statement.getResultSet();
    }

    protected abstract boolean execute(String query, Statement statement) throws SQLException;

    public static class StatementControllerExecuteTest extends StatementControllerExecute2Test<Integer> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query);
        }

        @Override
        protected Integer executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update);
        }
    }

    public static class StatementControllerExecuteReturnGeneratedKeysTest extends StatementControllerExecute2Test<Integer> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, Statement.RETURN_GENERATED_KEYS);
        }

        @Override
        protected Integer executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update, Statement.RETURN_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteNoGeneratedKeysTest extends StatementControllerExecute2Test<Long> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, Statement.NO_GENERATED_KEYS);
        }

        @Override
        protected Long executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeLargeUpdate(update, Statement.NO_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteEmptyColumnIndexesTest extends StatementControllerExecute2Test<Integer> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new int[0]);
        }

        @Override
        protected Integer executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update, new int[0]);
        }
    }

    public static class StatementControllerExecuteEmptyColumnKeysTest extends StatementControllerExecute2Test<Integer> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new String[0]);
        }

        @Override
        protected Integer executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeUpdate(update, new String[0]);
        }
    }

    public static class StatementControllerExecuteLargeReturnGeneratedKeysTest extends StatementControllerExecute2Test<Long> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query);
        }
        @Override
        protected Long executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeLargeUpdate(update, Statement.RETURN_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteLargeNoGeneratedKeysTest extends StatementControllerExecute2Test<Long> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, Statement.NO_GENERATED_KEYS);
        }

        @Override
        protected Long executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeLargeUpdate(update, Statement.NO_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteLargeEmptyColumnIndexesTest extends StatementControllerExecute2Test<Long> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new int[0]);
        }

        @Override
        protected Long executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeLargeUpdate(update, new int[0]);
        }
    }

    public static class StatementControllerExecuteLargeEmptyColumnKeysTest extends StatementControllerExecute2Test<Long> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new String[0]);
        }

        @Override
        protected Long executeUpdate(Connection conn, String update) throws SQLException {
            return conn.createStatement().executeLargeUpdate(update, new String[0]);
        }
    }
}

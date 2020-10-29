package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

abstract class StatementControllerExecute2Test<T> extends StatementControllerTestBase<Statement> {
    @SafeVarargs
    @Override
    protected final ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<Statement, SQLException>... setters) throws SQLException {
        Statement statement = conn.createStatement();
        runSetters(statement, setters);
        assertTrue(execute(query, statement));
        return statement.getResultSet();
    }

    protected abstract boolean execute(String query, Statement statement) throws SQLException;

    public static class StatementControllerExecuteReturnGeneratedKeysTest extends StatementControllerExecute2Test<Integer> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, Statement.RETURN_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteNoGeneratedKeysTest extends StatementControllerExecute2Test<Integer> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, Statement.RETURN_GENERATED_KEYS);
        }
    }

    public static class StatementControllerExecuteEmptyColumnIndexesTest extends StatementControllerExecute2Test<int[]> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new int[0]);
        }
    }

    public static class StatementControllerExecuteEmptyColumnKeysTest extends StatementControllerExecute2Test<String[]> {
        @Override
        protected boolean execute(String query, Statement statement) throws SQLException {
            return statement.execute(query, new String[0]);
        }
    }
}

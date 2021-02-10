package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.nosqldriver.jdbc.http.AssertUtils.ResultSetAssertMode.CALL_ALL_GETTERS;
import static com.nosqldriver.jdbc.http.AssertUtils.ResultSetAssertMode.RANGE_EXCEPTION_MESSAGE;
import static java.util.Arrays.asList;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

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

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypesCallAllGetters(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypes(nativeUrl, "select * from test_all_types", null, asList(CALL_ALL_GETTERS, RANGE_EXCEPTION_MESSAGE));
    }

}

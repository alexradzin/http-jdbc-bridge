package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class PreparedStatementControllerExecuteQueryTest extends StatementControllerTestBase<PreparedStatement> {
    @Override
    protected Statement createStatement(Connection conn, String db) throws SQLException {
        return conn.prepareStatement(getCheckConnectivityQuery(db));
    }

    @Override
    protected ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<PreparedStatement, SQLException>... setters) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(query);
        runSetters(ps, setters);
        return ps.executeQuery();
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithIndexedArgumentsWithAllTypes(String nativeUrl) throws SQLException {
        selectTableWithAllTypes(nativeUrl, "select * from test_all_types where i=?", null, preparedStatement -> preparedStatement.setInt(1, 12345));
    }

    @Override
    protected int executeUpdate(Connection conn, String update) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(update);
        return ps.executeUpdate();
    }
}

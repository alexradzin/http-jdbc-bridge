package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class PreparedStatementControllerTest extends StatementControllerTestBase<PreparedStatement> {
    @Override
    protected Statement createStatement(Connection conn, String db) throws SQLException {
        return conn.prepareStatement(getCheckConnectivityQuery(db));
    }

    @Override
    protected ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<PreparedStatement, SQLException>... setters) throws SQLException {
        PreparedStatement ps = conn.prepareStatement(query);
        for (ThrowingConsumer<PreparedStatement, SQLException> setter : setters) {
            setter.accept(ps);
        }
        return ps.executeQuery();
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithIndexedArgumentsWithAllTypes(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypes(nativeUrl, "select * from test_all_types where i=?", preparedStatement -> preparedStatement.setInt(1, 12345));
    }
}

package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class StatementControllerTest extends StatementControllerTestBase<Statement> {
    @SafeVarargs
    @Override
    protected final ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<Statement, SQLException>... setters) throws SQLException {
        Statement statement = conn.createStatement();
        for (ThrowingConsumer<Statement, SQLException> setter : setters) {
            setter.accept(statement);
        }
        return statement.executeQuery(query);
    }
}

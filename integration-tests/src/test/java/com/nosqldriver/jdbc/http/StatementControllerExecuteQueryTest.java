package com.nosqldriver.jdbc.http;

import com.nosqldriver.jdbc.http.AssertUtils.GettersSupplier;
import com.nosqldriver.util.function.ThrowingConsumer;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.nosqldriver.jdbc.http.AssertUtils.ResultSetAssertMode.RANGE_EXCEPTION_MESSAGE;
import static java.util.Collections.singletonList;
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
        selectTableWithAllTypesGetClob(nativeUrl, GettersSupplier.ALL);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypesGetClob(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypesGetClob(nativeUrl, GettersSupplier.CLOB);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypesGetAsciiStream(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypesGetClob(nativeUrl, GettersSupplier.ASCII_STREAM);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypesGetBinaryStream(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypesGetClob(nativeUrl, GettersSupplier.BINARY_STREAM);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypesGetUnicodeStream(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypesGetClob(nativeUrl, GettersSupplier.UNICODE_STREAM);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypesGetCharacterStream(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypesGetClob(nativeUrl, GettersSupplier.CHARACTER_STREAM);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypesGetNCharacterStream(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypesGetClob(nativeUrl, GettersSupplier.NCHARACTER_STREAM);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectAndUpdateByIndex(String nativeUrl) throws SQLException {
        selectTableWithAllTypesAndUpdate(nativeUrl, "select i, si, bi, f, dcml, r from test_all_types",
                singletonList(RANGE_EXCEPTION_MESSAGE), GettersSupplier.ALL,
                rs -> rs.updateInt(1, 54321),
                rs -> rs.updateShort(2, (short)321),
                rs -> rs.updateLong(3, 987654321),
                rs -> rs.updateFloat(4, 2.7f),
                rs -> rs.updateFloat(5, 2.7f),
                rs -> rs.updateDouble(6, 3.1415926)
        );
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectAndUpdateByName(String nativeUrl) throws SQLException {
        selectTableWithAllTypesAndUpdate(nativeUrl, "select i, si, bi, f, dcml, r from test_all_types",
                singletonList(RANGE_EXCEPTION_MESSAGE), GettersSupplier.ALL,
                rs -> rs.updateInt("i", 54321),
                rs -> rs.updateShort("si", (short)321),
                rs -> rs.updateLong("bi", 987654321),
                rs -> rs.updateFloat("f", 2.7f),
                rs -> rs.updateFloat("dcml", 2.7f),
                rs -> rs.updateDouble("r", 3.1415926)
        );
    }

    private void selectTableWithAllTypesGetClob(String nativeUrl, GettersSupplier gettersSupplier) throws SQLException, IOException {
        selectTableWithAllTypes(nativeUrl, "select * from test_all_types", null, singletonList(RANGE_EXCEPTION_MESSAGE), gettersSupplier);
    }
}

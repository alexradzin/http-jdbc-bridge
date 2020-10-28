package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static com.nosqldriver.jdbc.http.AssertUtils.assertGettersAndSetters;
import static com.nosqldriver.jdbc.http.AssertUtils.assertResultSet;
import static java.lang.String.format;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.FETCH_UNKNOWN;
import static java.sql.Statement.CLOSE_ALL_RESULTS;
import static java.sql.Statement.CLOSE_CURRENT_RESULT;
import static java.sql.Statement.KEEP_CURRENT_RESULT;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public abstract class StatementControllerTestBase<T extends Statement> extends ControllerTestBase {
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);
        String db = db(nativeUrl);
        Statement httpStatement = createStatement(httpConn, db);
        Statement nativeStatement = createStatement(nativeConn, db);

        assertSame(nativeConn, nativeStatement.getConnection());
        assertSame(httpConn, httpStatement.getConnection());

        Collection<AbstractMap.SimpleEntry<String, ThrowingFunction<Statement, ?,  SQLException>>> getters = Arrays.asList(
                new AbstractMap.SimpleEntry<>("getMoreResults", Statement::getMoreResults),
                new AbstractMap.SimpleEntry<>("getGeneratedKeys", Statement::getGeneratedKeys),
                new AbstractMap.SimpleEntry<>("getLargeUpdateCount", Statement::getLargeUpdateCount),
                new AbstractMap.SimpleEntry<>("getResultSet", Statement::getResultSet),
                new AbstractMap.SimpleEntry<>("getResultSetConcurrency", Statement::getResultSetConcurrency),
                new AbstractMap.SimpleEntry<>("getResultSetHoldability", Statement::getResultSetHoldability),
                new AbstractMap.SimpleEntry<>("getResultSetType", Statement::getResultSetType),
                new AbstractMap.SimpleEntry<>("getUpdateCount", Statement::getUpdateCount),
                new AbstractMap.SimpleEntry<>("getWarnings", Statement::getWarnings),
                new AbstractMap.SimpleEntry<>("enquoteLiteral()", s -> s.enquoteLiteral("")),
                new AbstractMap.SimpleEntry<>("enquoteLiteral(abc)", s -> s.enquoteLiteral("abc")),
                new AbstractMap.SimpleEntry<>("enquoteIdentifier", s -> s.enquoteIdentifier("abc", false)),
                new AbstractMap.SimpleEntry<>("enquoteIdentifier", s -> s.enquoteIdentifier("abc", true)),
                new AbstractMap.SimpleEntry<>("isSimpleIdentifier", s -> s.isSimpleIdentifier("abc")),
                new AbstractMap.SimpleEntry<>("enquoteNCharLiteral", s -> s.enquoteNCharLiteral("")),
                new AbstractMap.SimpleEntry<>("enquoteNCharLiteral", s -> s.enquoteNCharLiteral("abc"))
        );

        for (Map.Entry<String, ThrowingFunction<Statement, ?, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingFunction<Statement, ?, SQLException> f = getter.getValue();
            AssertUtils.assertCall(f, nativeStatement, httpStatement, name);
        }
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void gettersAndSetters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);
        String db = db(nativeUrl);
        Statement httpStatement = createStatement(httpConn, db);
        Statement nativeStatement = createStatement(nativeConn, db);

        Collection<Map.Entry<String, Map.Entry<ThrowingFunction<Statement, ?, SQLException>, ThrowingConsumer<Statement, SQLException>>>> functions = Arrays.asList(
                new AbstractMap.SimpleEntry<>("FetchDirection", new AbstractMap.SimpleEntry<>(Statement::getFetchDirection, s -> s.setFetchDirection(0))),
                new AbstractMap.SimpleEntry<>("FetchDirection", new AbstractMap.SimpleEntry<>(Statement::getFetchDirection, s -> s.setFetchDirection(FETCH_FORWARD))),
                new AbstractMap.SimpleEntry<>("FetchDirection", new AbstractMap.SimpleEntry<>(Statement::getFetchDirection, s -> s.setFetchDirection(FETCH_UNKNOWN))),

                new AbstractMap.SimpleEntry<>("FetchSize", new AbstractMap.SimpleEntry<>(Statement::getFetchSize, s -> s.setFetchSize(0))),
                new AbstractMap.SimpleEntry<>("FetchSize", new AbstractMap.SimpleEntry<>(Statement::getFetchSize, s -> s.setFetchSize(100))),

                new AbstractMap.SimpleEntry<>("MaxRows", new AbstractMap.SimpleEntry<>(Statement::getMaxRows, s -> s.setMaxRows(0))),
                new AbstractMap.SimpleEntry<>("MaxRows", new AbstractMap.SimpleEntry<>(Statement::getMaxRows, s -> s.setMaxRows(100))),

                new AbstractMap.SimpleEntry<>("LargeMaxRows", new AbstractMap.SimpleEntry<>(Statement::getLargeMaxRows, s -> s.setLargeMaxRows(0))),
                new AbstractMap.SimpleEntry<>("LargeMaxRows", new AbstractMap.SimpleEntry<>(Statement::getLargeMaxRows, s -> s.setLargeMaxRows(99999L))),

                new AbstractMap.SimpleEntry<>("MaxFieldSize", new AbstractMap.SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(0))),
                new AbstractMap.SimpleEntry<>("MaxFieldSize", new AbstractMap.SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(16))),
                new AbstractMap.SimpleEntry<>("MaxFieldSize", new AbstractMap.SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(256))),
                new AbstractMap.SimpleEntry<>("MaxFieldSize", new AbstractMap.SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(512))),

                new AbstractMap.SimpleEntry<>("QueryTimeout", new AbstractMap.SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(0))),
                new AbstractMap.SimpleEntry<>("QueryTimeout", new AbstractMap.SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(10))),
                new AbstractMap.SimpleEntry<>("QueryTimeout", new AbstractMap.SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(-1))),
                new AbstractMap.SimpleEntry<>("QueryTimeout", new AbstractMap.SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(1000))),
                new AbstractMap.SimpleEntry<>("Poolable(false)", new AbstractMap.SimpleEntry<>(Statement::isPoolable, s -> s.setPoolable(false))),
                new AbstractMap.SimpleEntry<>("Poolable(true)", new AbstractMap.SimpleEntry<>(Statement::isPoolable, s -> s.setPoolable(true))),
                new AbstractMap.SimpleEntry<>("CloseOnCompletion", new AbstractMap.SimpleEntry<>(Statement::isCloseOnCompletion, Statement::closeOnCompletion)),

                new AbstractMap.SimpleEntry<>("Close", new AbstractMap.SimpleEntry<>(Statement::isClosed, Statement::close))

        );

        assertGettersAndSetters(functions, nativeStatement, httpStatement);
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void voidFunctions(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);
        String db = db(nativeUrl);
        Statement httpStatement = createStatement(httpConn, db);
        Statement nativeStatement = createStatement(nativeConn, db);

        Collection<AbstractMap.SimpleEntry<String, ThrowingConsumer<Statement, SQLException>>> getters = Arrays.asList(
                new AbstractMap.SimpleEntry<>("setEscapeProcessing(true)", s -> s.setEscapeProcessing(true)),
                new AbstractMap.SimpleEntry<>("setEscapeProcessing(false)", s -> s.setEscapeProcessing(false)),
                new AbstractMap.SimpleEntry<>("setCursorName(my)", s -> s.setCursorName("my")),
                new AbstractMap.SimpleEntry<>("cancel", Statement::cancel),
                new AbstractMap.SimpleEntry<>("addBatch", s -> s.addBatch(getCheckConnectivityQuery(db))),
                new AbstractMap.SimpleEntry<>("clearBatch", Statement::clearBatch),
                new AbstractMap.SimpleEntry<>("clearWarnings", Statement::clearWarnings),
                new AbstractMap.SimpleEntry<>("closeOnCompletion", Statement::closeOnCompletion),
                new AbstractMap.SimpleEntry<>("getMoreResults(CLOSE_CURRENT_RESULT)", s -> s.getMoreResults(CLOSE_CURRENT_RESULT)),
                new AbstractMap.SimpleEntry<>("getMoreResults(KEEP_CURRENT_RESULT)", s -> s.getMoreResults(KEEP_CURRENT_RESULT)),
                new AbstractMap.SimpleEntry<>("getMoreResults(CLOSE_ALL_RESULTS)", s -> s.getMoreResults(CLOSE_ALL_RESULTS)),
                new AbstractMap.SimpleEntry<>("executeBatch", Statement::executeBatch),
                new AbstractMap.SimpleEntry<>("executeLargeBatch", Statement::executeLargeBatch)
        );

        for (Map.Entry<String, ThrowingConsumer<Statement, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingConsumer<Statement, SQLException> f = getter.getValue();
            AssertUtils.assertCall(f, nativeStatement, httpStatement, name);
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectEmptyTableWithAllTypes(String nativeUrl) throws SQLException, IOException {
        selectEmptyTableWithAllTypes(nativeUrl, "select * from test_all_types");
    }

    protected void selectEmptyTableWithAllTypes(String nativeUrl, String query, ThrowingConsumer<T, SQLException>... setters) throws SQLException, IOException {
        select(nativeUrl,
                new String[] {sqlScript(db(nativeUrl), "create.table.all-types.sql")},
                query,
                new String[] {"drop table test_all_types"},
                setters);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypes(String nativeUrl) throws SQLException, IOException {
        selectTableWithAllTypes(nativeUrl, "select * from test_all_types");
    }

    protected void selectTableWithAllTypes(String nativeUrl, String query, ThrowingConsumer<T, SQLException> ... setters) throws SQLException {
        String db = db(nativeUrl);
        select(nativeUrl,
                Stream.of("create.table.all-types.sql", "insert.all-types.sql").map(f -> sqlScript(db, f)).toArray(String[]::new),
                query,
                new String[] {"drop table test_all_types"},
                setters);
    }

    protected Statement createStatement(Connection conn, String db) throws SQLException {
        return conn.createStatement();
    }

    private void select(String nativeUrl, String[] before, String query, String[] after, ThrowingConsumer<T, SQLException>... setters) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);
        try {
            for (String sql : before) {
                nativeConn.createStatement().execute(sql);
            }
            try (ResultSet nativeRs = executeQuery(nativeConn, query, setters); ResultSet httpRs = executeQuery(httpConn, query, setters)) {
                assertResultSet(nativeRs, httpRs, query, Integer.MAX_VALUE);
            }
        } finally {
            for (String sql : after) {
                nativeConn.createStatement().execute(sql);
            }
        }
    }

    private String sqlScript(String db, String file) {
        try {
            return new String(getClass().getResourceAsStream(format("/sql/%s/%s", db, file)).readAllBytes());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected abstract ResultSet executeQuery(Connection conn, String query, ThrowingConsumer<T, SQLException>... setters) throws SQLException;
}
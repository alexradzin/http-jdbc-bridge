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
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Stream;

import static com.nosqldriver.jdbc.http.AssertUtils.assertGettersAndSetters;
import static com.nosqldriver.jdbc.http.AssertUtils.assertResultSet;
import static java.lang.String.format;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.FETCH_UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class StatementControllerTest extends ControllerTestBase {
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);
        Statement httpStatement = createStatement(httpConn);
        Statement nativeStatement = createStatement(nativeConn);

        assertSame(nativeConn, nativeStatement.getConnection());
        assertSame(httpConn, httpStatement.getConnection());

        Collection<SimpleEntry<String, ThrowingFunction<Statement, ?,  SQLException>>> getters = Arrays.asList(
                new SimpleEntry<>("getMoreResults", Statement::getMoreResults),
                new SimpleEntry<>("getGeneratedKeys", Statement::getGeneratedKeys),
                new SimpleEntry<>("getLargeUpdateCount", Statement::getLargeUpdateCount),
                new SimpleEntry<>("getResultSet", Statement::getResultSet),
                new SimpleEntry<>("getResultSetConcurrency", Statement::getResultSetConcurrency),
                new SimpleEntry<>("getResultSetHoldability", Statement::getResultSetHoldability),
                new SimpleEntry<>("getResultSetType", Statement::getResultSetType),
                new SimpleEntry<>("getUpdateCount", Statement::getUpdateCount),
                new SimpleEntry<>("getWarnings", Statement::getWarnings)
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
        Statement httpStatement = createStatement(httpConn);
        Statement nativeStatement = createStatement(nativeConn);

        Collection<Map.Entry<String, Map.Entry<ThrowingFunction<Statement, ?, SQLException>, ThrowingConsumer<Statement, SQLException>>>> functions = Arrays.asList(
                new SimpleEntry<>("FetchDirection", new SimpleEntry<>(Statement::getFetchDirection, s -> s.setFetchDirection(0))),
                new SimpleEntry<>("FetchDirection", new SimpleEntry<>(Statement::getFetchDirection, s -> s.setFetchDirection(FETCH_FORWARD))),
                new SimpleEntry<>("FetchDirection", new SimpleEntry<>(Statement::getFetchDirection, s -> s.setFetchDirection(FETCH_UNKNOWN))),

                new SimpleEntry<>("FetchSize", new SimpleEntry<>(Statement::getFetchSize, s -> s.setFetchSize(0))),
                new SimpleEntry<>("FetchSize", new SimpleEntry<>(Statement::getFetchSize, s -> s.setFetchSize(100))),

                new SimpleEntry<>("MaxRows", new SimpleEntry<>(Statement::getMaxRows, s -> s.setMaxRows(0))),
                new SimpleEntry<>("MaxRows", new SimpleEntry<>(Statement::getMaxRows, s -> s.setMaxRows(100))),

                new SimpleEntry<>("LargeMaxRows", new SimpleEntry<>(Statement::getLargeMaxRows, s -> s.setLargeMaxRows(0))),
                new SimpleEntry<>("LargeMaxRows", new SimpleEntry<>(Statement::getLargeMaxRows, s -> s.setLargeMaxRows(99999L))),

                new SimpleEntry<>("MaxFieldSize", new SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(0))),
                new SimpleEntry<>("MaxFieldSize", new SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(16))),
                new SimpleEntry<>("MaxFieldSize", new SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(256))),
                new SimpleEntry<>("MaxFieldSize", new SimpleEntry<>(Statement::getMaxFieldSize, s -> s.setMaxFieldSize(512))),

                new SimpleEntry<>("QueryTimeout", new SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(0))),
                new SimpleEntry<>("QueryTimeout", new SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(10))),
                new SimpleEntry<>("QueryTimeout", new SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(-1))),
                new SimpleEntry<>("QueryTimeout", new SimpleEntry<>(Statement::getQueryTimeout, s -> s.setQueryTimeout(1000))),

                new SimpleEntry<>("Close", new SimpleEntry<>(Statement::isClosed, Statement::close))

        );

        assertGettersAndSetters(functions, nativeStatement, httpStatement);
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void voidFunctions(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);
        Statement httpStatement = createStatement(httpConn);
        Statement nativeStatement = createStatement(nativeConn);

        Collection<SimpleEntry<String, ThrowingConsumer<Statement, SQLException>>> getters = Arrays.asList(
                new SimpleEntry<>("clearBatch", Statement::clearBatch),
                new SimpleEntry<>("clearWarnings", Statement::clearWarnings),
                new SimpleEntry<>("closeOnCompletion", Statement::closeOnCompletion),
                new SimpleEntry<>("executeBatch", Statement::executeBatch),
                new SimpleEntry<>("executeLargeBatch", Statement::executeLargeBatch)
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
        select(nativeUrl,
                new String[] {sqlScript(db(nativeUrl), "create.table.all-types.sql")},
                "select * from test_all_types",
                new String[] {"drop table test_all_types"});
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void selectTableWithAllTypes(String nativeUrl) throws SQLException, IOException {
        String db = db(nativeUrl);
        select(nativeUrl,
                Stream.of("create.table.all-types.sql", "insert.all-types.sql").map(f -> sqlScript(db, f)).toArray(String[]::new),
                "select * from test_all_types",
                new String[] {"drop table test_all_types"});
    }


    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.createStatement();
    }

    protected ResultSet executeQuery(Connection conn, String query) throws SQLException {
        return conn.createStatement().executeQuery(query);
    }

    private void select(String nativeUrl, String[] before, String query, String[] after) throws SQLException, IOException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);
        for (String sql : before) {
            nativeConn.createStatement().execute(sql);
        }
        try {
            assertResultSet(executeQuery(nativeConn, query), executeQuery(httpConn, query), query);
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

    private String db(String url) {
        return url.split(":")[1];
    }
}

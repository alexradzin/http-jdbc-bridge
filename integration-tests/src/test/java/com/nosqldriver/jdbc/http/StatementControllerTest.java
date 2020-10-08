package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import spark.Spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.nosqldriver.jdbc.http.AssertUtils.assertGettersAndSetters;
import static java.lang.String.format;
import static java.sql.ResultSet.FETCH_FORWARD;
import static java.sql.ResultSet.FETCH_UNKNOWN;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class StatementControllerTest {
    private static final String httpUrl = "http://localhost:8080";

    @BeforeAll
    static void beforeAll() {
        Spark.port(8080);
        new DriverController(new HashMap<>(), new ObjectMapper());
        Spark.awaitInitialization();
    }

    @AfterAll
    static void afterAll() {
        Spark.stop();
        Spark.awaitStop();
    }

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


    protected Statement createStatement(Connection conn) throws SQLException {
        return conn.createStatement();
    }
}

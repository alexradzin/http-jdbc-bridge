package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import spark.Spark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class DriverControllerTest {
    private static final String httpUrl = "http://localhost:8080";

    @BeforeAll
    static void beforeAll() {
        spark.Spark.port(8080);
        new DriverController(new HashMap<>(), new ObjectMapper());
        spark.Spark.awaitInitialization();
    }

    @AfterAll
    static void afterAll() {
        Spark.stop();
        Spark.awaitStop();
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createAndCloseConnection(String nativeUrl) throws SQLException {
        assertCreateAndCloseConnection(format("%s#%s", httpUrl, nativeUrl));
        assertCreateAndCloseConnection(nativeUrl);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {"jdbc:unsupported:foo", httpUrl + "#" + "jdbc:unsupported:foo"})
    void createConnectionViaDriverManagerUsingUnsupportedJdbcUrl(String url) throws SQLException {
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {"jdbc:unsupported:foo", httpUrl + "#" + "jdbc:unsupported:foo"})
    void getDriverUsingUnsupportedJdbcUrl(String url) {
        assertThrows(SQLException.class, () -> DriverManager.getDriver(url));
    }

    @Test
    void getUsingUnsupportedJdbcUrlConnectionDirectly() throws SQLException {
        assertNull(new HttpDriver().connect(httpUrl + "#" + "jdbc:unsupported:foo", null));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        Collection<AbstractMap.SimpleEntry<String, ThrowingFunction<Connection, ?,  SQLException>>> getters = Arrays.asList(
                new AbstractMap.SimpleEntry<>("getClientInfo", Connection::getClientInfo),
                new AbstractMap.SimpleEntry<>("getCatalog", Connection::getCatalog),
                new AbstractMap.SimpleEntry<>("getSchema", Connection::getSchema),
                new AbstractMap.SimpleEntry<>("getAutoCommit", Connection::getAutoCommit),
                new AbstractMap.SimpleEntry<>("getHoldability", Connection::getHoldability),
                new AbstractMap.SimpleEntry<>("getNetworkTimeout", Connection::getNetworkTimeout),
                new AbstractMap.SimpleEntry<>("getTransactionIsolation", Connection::getTransactionIsolation),
                new AbstractMap.SimpleEntry<>("getTypeMap", Connection::getTypeMap),
                new AbstractMap.SimpleEntry<>("getWarnings", Connection::getWarnings),
                //new AbstractMap.SimpleEntry<>("setSavepoint", Connection::setSavepoint),
                new AbstractMap.SimpleEntry<>("getClientInfo", c -> c.getClientInfo("")),
                new AbstractMap.SimpleEntry<>("getClientInfo", c -> c.getClientInfo("foo")),
                new AbstractMap.SimpleEntry<>("getWarnings", c -> c.isValid(0))
        );

        for (Map.Entry<String, ThrowingFunction<Connection, ?, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingFunction<Connection, ?, SQLException> f = getter.getValue();
            assertEquals(f.apply(nativeConn), f.apply(httpConn), name);
        }
    }


    private void assertCreateAndCloseConnection(String url) throws SQLException {
        Connection conn = DriverManager.getConnection(url);
        assertNotNull(conn);
        conn.close();
    }

}

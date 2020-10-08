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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class ConnectionControllerTest {
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

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void create(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        String query = "select 1";
        Collection<AbstractMap.SimpleEntry<String, ThrowingFunction<Connection, ?,  SQLException>>> functions = Arrays.asList(
                new AbstractMap.SimpleEntry<>("createStatement", Connection::createStatement),
                new AbstractMap.SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)),
                new AbstractMap.SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)),
                new AbstractMap.SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE)),
                new AbstractMap.SimpleEntry<>("createBlob", Connection::createBlob),
                new AbstractMap.SimpleEntry<>("createClob", Connection::createClob),
                new AbstractMap.SimpleEntry<>("createNClob", Connection::createNClob),
                new AbstractMap.SimpleEntry<>("createArrayOf", c -> c.createArrayOf("int", new Object[0])),
                new AbstractMap.SimpleEntry<>("createSQLXML", Connection::createSQLXML),

                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE)),

                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE)),

                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, ResultSet.HOLD_CURSORS_OVER_COMMIT)),

                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT)),
                new AbstractMap.SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, ResultSet.CLOSE_CURSORS_AT_COMMIT)),


                new AbstractMap.SimpleEntry<>("createSQLXML", Connection::createSQLXML)
        );

        for (Map.Entry<String, ThrowingFunction<Connection, ?, SQLException>> function : functions) {
            String name = function.getKey();
            ThrowingFunction<Connection, ?, SQLException> f = function.getValue();
            AssertUtils.assertCall(f, nativeConn, httpConn, name);
        }
    }
}

package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.Executors;

import static com.nosqldriver.jdbc.http.AssertUtils.assertCall;
import static com.nosqldriver.jdbc.http.AssertUtils.assertGettersAndSetters;
import static java.lang.String.format;
import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.CONCUR_UPDATABLE;
import static java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;
import static java.sql.ResultSet.TYPE_SCROLL_INSENSITIVE;
import static java.sql.ResultSet.TYPE_SCROLL_SENSITIVE;
import static java.sql.Statement.NO_GENERATED_KEYS;
import static java.sql.Statement.RETURN_GENERATED_KEYS;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class ConnectionControllerTest extends ControllerTestBase {
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        Collection<Entry<String, ThrowingFunction<Connection, ?,  SQLException>>> getters = Arrays.asList(
                new SimpleEntry<>("getClientInfo", Connection::getClientInfo),
                new SimpleEntry<>("getCatalog", Connection::getCatalog),
                new SimpleEntry<>("getSchema", Connection::getSchema),
                new SimpleEntry<>("getAutoCommit", Connection::getAutoCommit),
                new SimpleEntry<>("getHoldability", Connection::getHoldability),
                new SimpleEntry<>("getNetworkTimeout", Connection::getNetworkTimeout),
                new SimpleEntry<>("getTransactionIsolation", Connection::getTransactionIsolation),
                new SimpleEntry<>("getTypeMap", Connection::getTypeMap),
                new SimpleEntry<>("getWarnings", Connection::getWarnings),
                //new AbstractMap.SimpleEntry<>("setSavepoint", Connection::setSavepoint),
                new SimpleEntry<>("getClientInfo", c -> c.getClientInfo("")),
                new SimpleEntry<>("getClientInfo", c -> c.getClientInfo("foo")),
                new SimpleEntry<>("getWarnings", c -> c.isValid(0))
        );

        for (Entry<String, ThrowingFunction<Connection, ?, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingFunction<Connection, ?, SQLException> f = getter.getValue();
            assertCall(f, nativeConn, httpConn, name);
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void gettersAndSetters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        Collection<Map.Entry<String, Map.Entry<ThrowingFunction<Connection, ?, SQLException>, ThrowingConsumer<Connection, SQLException>>>> functions = Arrays.asList(
                new SimpleEntry<>("TypeMap", new SimpleEntry<>(Connection::getTypeMap, s -> s.setTypeMap(Collections.emptyMap()))),
                new SimpleEntry<>("ClientInfo", new SimpleEntry<>(Connection::getClientInfo, s -> s.setClientInfo(new Properties()))),
                new SimpleEntry<>("ClientInfo", new SimpleEntry<>(Connection::getClientInfo, s -> s.setClientInfo("foo", "bar"))),
                new SimpleEntry<>("Holdability", new SimpleEntry<>(Connection::getHoldability, s -> s.setHoldability(HOLD_CURSORS_OVER_COMMIT))),
                new SimpleEntry<>("Holdability", new SimpleEntry<>(Connection::getHoldability, s -> s.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT))),
                new SimpleEntry<>("NetworkTimeout", new SimpleEntry<>(Connection::getNetworkTimeout, s -> s.setNetworkTimeout(Executors.newSingleThreadExecutor(), 0))),
                new SimpleEntry<>("NetworkTimeout", new SimpleEntry<>(Connection::getTransactionIsolation, s -> s.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED))),
                new SimpleEntry<>("NetworkTimeout", new SimpleEntry<>(Connection::getTransactionIsolation, s -> s.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED))),
                new SimpleEntry<>("NetworkTimeout", new SimpleEntry<>(Connection::getTransactionIsolation, s -> s.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ))),
                new SimpleEntry<>("NetworkTimeout", new SimpleEntry<>(Connection::getTransactionIsolation, s -> s.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE))),
                new SimpleEntry<>("AutoCommit", new SimpleEntry<>(Connection::getAutoCommit, s -> s.setAutoCommit(false))),
                new SimpleEntry<>("AutoCommit", new SimpleEntry<>(Connection::getAutoCommit, s -> s.setAutoCommit(true))),
                new SimpleEntry<>("Catalog", new SimpleEntry<>(Connection::getCatalog, s -> s.setCatalog("my_catalog"))),
                new SimpleEntry<>("ReadOnly", new SimpleEntry<>(Connection::isReadOnly, s -> s.setReadOnly(false))),
                new SimpleEntry<>("ReadOnly", new SimpleEntry<>(Connection::isReadOnly, s -> s.setReadOnly(true))),
                new SimpleEntry<>("Schema", new SimpleEntry<>(Connection::getSchema, s -> s.setSchema("my_schema"))),
                new SimpleEntry<>("Close", new SimpleEntry<>(Connection::isClosed, Connection::close))
        );


        assertGettersAndSetters(functions, nativeConn, httpConn);
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void create(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        String query = getCheckConnectivityQuery(db(nativeUrl));
        Collection<SimpleEntry<String, ThrowingFunction<Connection, ?,  SQLException>>> functions = Arrays.asList(
                new SimpleEntry<>("createStatement", Connection::createStatement),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE)),

                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),

                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("createStatement", c -> c.createStatement(TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),

                new SimpleEntry<>("createBlob", Connection::createBlob),
                new SimpleEntry<>("createClob", Connection::createClob),
                new SimpleEntry<>("createNClob", Connection::createNClob),
                new SimpleEntry<>("createArrayOf", c -> c.createArrayOf("int", new Object[0])),
                new SimpleEntry<>("createSQLXML", Connection::createSQLXML),
                new SimpleEntry<>("createStruct", c -> c.createStruct("person", new Object[] {"first_name", "last_name"})),

                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE)),

                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),

                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),

                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, RETURN_GENERATED_KEYS)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, NO_GENERATED_KEYS)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, new int[0])),
                new SimpleEntry<>("prepareStatement", c -> c.prepareStatement(query, new String[0])),

                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE)),

                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, HOLD_CURSORS_OVER_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, HOLD_CURSORS_OVER_COMMIT)),

                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_FORWARD_ONLY, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_INSENSITIVE, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_READ_ONLY, CLOSE_CURSORS_AT_COMMIT)),
                new SimpleEntry<>("prepareStatement", c -> c.prepareCall(query, TYPE_SCROLL_SENSITIVE, CONCUR_UPDATABLE, CLOSE_CURSORS_AT_COMMIT)),

                new SimpleEntry<>("createSQLXML", Connection::createSQLXML)
        );

        for (Entry<String, ThrowingFunction<Connection, ?, SQLException>> function : functions) {
            String name = function.getKey();
            ThrowingFunction<Connection, ?, SQLException> f = function.getValue();
            assertCall(f, nativeConn, httpConn, name);
        }
    }
}

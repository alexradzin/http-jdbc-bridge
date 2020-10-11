package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class DriverControllerTest extends ControllerTestBase {
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createAndCloseConnection(String nativeUrl) throws SQLException {
        assertCreateAndCloseConnection(format("%s#%s", httpUrl, nativeUrl));
        assertCreateAndCloseConnection(nativeUrl);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {"jdbc:unsupported:foo", httpUrl + "#" + "jdbc:unsupported:foo"})
    void createConnectionViaDriverManagerUsingUnsupportedJdbcUrl(String url) {
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
    void createAndCloseConnectionWithPredefinedUrl(String nativeUrl) throws SQLException {
        Properties props = new Properties();
        String db = nativeUrl.split(":")[1];
        props.setProperty("user", db);
        props.setProperty("password", db);
        assertCreateAndCloseConnection(httpUrl, props);
    }


    private void assertCreateAndCloseConnection(String url) throws SQLException {
        assertCreateAndCloseConnection(url, new Properties());
    }

    private void assertCreateAndCloseConnection(String url, Properties props) throws SQLException {
        Connection conn = DriverManager.getConnection(url, props);
        assertNotNull(conn);
        conn.close();
    }

}

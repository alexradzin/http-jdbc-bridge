package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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


    private void assertCreateAndCloseConnection(String url) throws SQLException {
        Connection conn = DriverManager.getConnection(url);
        assertNotNull(conn);
        conn.close();
    }

}

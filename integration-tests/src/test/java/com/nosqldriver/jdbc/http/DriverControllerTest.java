package com.nosqldriver.jdbc.http;

import com.gargoylesoftware.htmlunit.ScriptException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class DriverControllerTest extends ControllerTestBase {
    @BeforeEach
    @Override
    void initDb(TestInfo testInfo) {
    }

    @AfterEach
    @Override
    void cleanDb() {
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createAndCloseConnection(String nativeUrl) throws SQLException, IOException {
        assertCreateAndCloseConnection(format("%s#%s", httpUrl, nativeUrl));
        assertCreateAndCloseConnection(nativeUrl);
        executeJavaScript(nativeUrl);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {"jdbc:unsupported:foo", httpUrl + "#" + "jdbc:unsupported:foo"})
    void createConnectionViaDriverManagerUsingUnsupportedJdbcUrl(String url) {
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        assertThrows(ScriptException.class, () -> executeJavaScript(url));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {"jdbc:unsupported:foo", httpUrl + "#" + "jdbc:unsupported:foo"})
    void getDriverUsingUnsupportedJdbcUrl(String url) {
        assertThrows(SQLException.class, () -> DriverManager.getDriver(url));
        assertThrows(ScriptException.class, () -> executeJavaScript(url));
    }

    @Test
    void getUsingUnsupportedJdbcUrlConnectionDirectly() throws IOException {
        String url = httpUrl + "#" + "jdbc:unsupported:foo";
        assertNull(new HttpDriver().connect(url, null));
        executeJavaScript(url);
    }

    @Test
    void createConnectionWithExistingUserNotMappedToDatabaseButWithSymbolicReferenceToUnsupportedDb() {
        Properties props = new Properties();
        props.setProperty("user", "nodb");
        props.setProperty("password", "nopass");
        String url = format("%s#%s", httpUrl, "unsupported");
        assertEquals("No suitable driver found for " + url, assertThrows(SQLException.class, () -> assertCreateAndCloseConnection(url, props)).getMessage());
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void acceptsSupportedJdbcURL(String nativeUrl) throws SQLException {
        Driver driver = new HttpDriver();
        assertTrue(driver.acceptsURL(format("%s#%s", httpUrl, nativeUrl)));
    }

    @Test
    void acceptsUnsupportedJdbcURL() throws SQLException {
        Driver driver = new HttpDriver();
        assertFalse(driver.acceptsURL(format("%s#%s", httpUrl, "jdbc:unknown")));
    }

    private void assertCreateAndCloseConnection(String url) throws SQLException {
        assertCreateAndCloseConnection(url, new Properties());
    }

    private void assertCreateAndCloseConnection(String url, Properties props) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, props)) {
            assertNotNull(conn);
        }
    }
}

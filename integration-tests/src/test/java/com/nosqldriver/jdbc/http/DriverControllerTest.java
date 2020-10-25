package com.nosqldriver.jdbc.http;

import com.gargoylesoftware.htmlunit.ScriptException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class DriverControllerTest extends ControllerTestBase {
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createAndCloseConnection(String nativeUrl) throws SQLException, IOException {
        System.out.println("jjjjjjjjjjjjjjj=" + getClass().getResource("/HttpConnector.js"));
        assertCreateAndCloseConnection(format("%s#%s", httpUrl, nativeUrl));
        assertCreateAndCloseConnection(nativeUrl);
        executeJavaScript(nativeUrl);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {"jdbc:unsupported:foo", httpUrl + "#" + "jdbc:unsupported:foo"})
    void createConnectionViaDriverManagerUsingUnsupportedJdbcUrl(String url) throws IOException {
        System.out.println("CLASSPATH=" + ManagementFactory.getRuntimeMXBean().getClassPath());
        System.out.println("jjjjjjjjjjjjjjj=" + getClass().getResource("/HttpConnector.js"));
        assertThrows(SQLException.class, () -> DriverManager.getConnection(url));
        assertThrows(ScriptException.class, () -> executeJavaScript(url));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @ValueSource(strings = {"jdbc:unsupported:foo", httpUrl + "#" + "jdbc:unsupported:foo"})
    void getDriverUsingUnsupportedJdbcUrl(String url) {
        System.out.println("CLASSPATH=" + ManagementFactory.getRuntimeMXBean().getClassPath());
        System.out.println("jjjjjjjjjjjjjjj=" + getClass().getResource("/HttpConnector.js"));
        assertThrows(SQLException.class, () -> DriverManager.getDriver(url));
        assertThrows(ScriptException.class, () -> executeJavaScript(url));
    }

    @Test
    void getUsingUnsupportedJdbcUrlConnectionDirectly() throws SQLException, IOException {
        System.out.println("CLASSPATH=" + ManagementFactory.getRuntimeMXBean().getClassPath());
        System.out.println("jjjjjjjjjjjjjjj=" + getClass().getResource("/HttpConnector.js"));
        String url = httpUrl + "#" + "jdbc:unsupported:foo";
        assertNull(new HttpDriver().connect(url, null));
        executeJavaScript(url);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createAndCloseConnectionWithPredefinedUrl(String nativeUrl) throws SQLException, IOException {
        System.out.println("jjjjjjjjjjjjjjj=" + getClass().getResource("/HttpConnector.js"));
        Properties props = new Properties();
        String db = nativeUrl.split(":")[1];
        props.setProperty("user", db);
        props.setProperty("password", db);
        assertCreateAndCloseConnection(httpUrl, props);
        executeJavaScript(httpUrl, props);
    }

    @Test
    void createAndCloseConnectionWithPredefinedUrlWrongCredentials() throws SQLException, IOException {
        System.out.println("jjjjjjjjjjjjjjj=" + getClass().getResource("/HttpConnector.js"));
        Properties props = new Properties();
        props.setProperty("user", "nobody");
        props.setProperty("password", "wrong");
        assertEquals("Invalid username or password", assertThrows(FailedLoginException.class, () -> DriverManager.getConnection(httpUrl, props)).getMessage());
        assertThrows(ScriptException.class, () -> executeJavaScript(httpUrl, props));
    }

    @Test
    void createConnectionWithExistingUserNotMappedToDatabase() {
        System.out.println("jjjjjjjjjjjjjjj=" + getClass().getResource("/HttpConnector.js"));
        Properties props = new Properties();
        props.setProperty("user", "nodb");
        props.setProperty("password", "nopass");
        assertEquals("User nodb is not mapped to any JDBC URL", assertThrows(LoginException.class, () -> DriverManager.getConnection(httpUrl, props)).getMessage());
        assertThrows(ScriptException.class, () -> executeJavaScript(httpUrl, props));
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

package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;

import javax.security.auth.login.FailedLoginException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class DriverControllerWithLoginModuleTest extends ControllerTestBase {
    @BeforeEach
    @Override
    void initDb(TestInfo testInfo) {
    }

    @AfterEach
    @Override
    void cleanDb() {
    }

    @BeforeAll
    static void beforeAll() throws IOException {
        enableSecurityAuth();
        ControllerTestBase.beforeAll();
    }

    @AfterAll
    static void afterAll() throws IOException {
        disableSecurityAuth();
        ControllerTestBase.afterAll();
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createAndCloseConnectionWithPredefinedUrl(String nativeUrl) throws SQLException, IOException {
        Properties props = new Properties();
        String db = nativeUrl.split(":")[1];
        props.setProperty("user", db);
        props.setProperty("password", db);
        assertCreateAndCloseConnection(httpUrl, props);
        executeJavaScript(httpUrl, props);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createAndCloseConnectionWithPredefinedUrlAndWrongPassword(String nativeUrl) {
        Properties props = new Properties();
        String db = nativeUrl.split(":")[1];
        props.setProperty("user", db);
        props.setProperty("password", "this password is wrong");
        assertThrows(FailedLoginException.class, () -> assertCreateAndCloseConnection(httpUrl, props));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void createConnectionWithExistingUserNotMappedToDatabaseButWithSymbolicReferenceToDb(String nativeUrl) throws SQLException {
        String db = nativeUrl.split(":")[1]; // jdbc.properties contains mapping between user (equal to the db type, e.g. h2, derby etc)  and JDBC URL
        Properties props = new Properties();
        props.setProperty("user", "nodb");
        props.setProperty("password", "nopass");
        assertCreateAndCloseConnection(format("%s#%s", httpUrl, db), props);
    }

    private void assertCreateAndCloseConnection(String url, Properties props) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, props)) {
            assertNotNull(conn);
        }
    }
}

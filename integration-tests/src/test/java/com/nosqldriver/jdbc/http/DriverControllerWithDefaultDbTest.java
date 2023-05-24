package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class DriverControllerWithDefaultDbTest extends ControllerTestBase {
    private static String jdbcConf;

    @BeforeAll
    static void beforeAll() throws IOException {
        enableSecurityAuth();
        jdbcConf = System.setProperty("jdbc.conf", "src/test/resources/jdbc-with-default-db.properties");
        ControllerTestBase.beforeAll();
    }

    @AfterAll
    static void afterAll() throws IOException {
        disableSecurityAuth();
        ControllerTestBase.afterAll();
        if (jdbcConf == null) {
            System.getProperties().remove("jdbc.conf");
        } else {
            System.setProperty("jdbc.conf", jdbcConf);
        }
    }

    @BeforeEach
    @Override
    void initDb(TestInfo testInfo) {
    }

    @AfterEach
    @Override
    void cleanDb() {
    }

    @Test
    void createAndCloseConnectionToDefaultDb() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "nodb");
        props.setProperty("password", "nopass");
        assertCreateAndCloseConnection(httpUrl, props);
    }

    private void assertCreateAndCloseConnection(String url, Properties props) throws SQLException {
        try (Connection conn = DriverManager.getConnection(url, props)) {
            assertNotNull(conn);
        }
    }
}

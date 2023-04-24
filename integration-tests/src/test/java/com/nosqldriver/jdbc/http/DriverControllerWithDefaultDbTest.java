package com.nosqldriver.jdbc.http;

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
    @BeforeAll
    static void beforeAll() throws IOException {
        System.setProperty("jdbc.conf", "src/test/resources/jdbc-with-default-db.properties");
        ControllerTestBase.beforeAll();
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

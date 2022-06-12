package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import spark.Spark;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HttpDriverMainTest {
    @Test
    void noArgs() throws IOException, SQLException {
        main(new String[0], "http://localhost:8080");
    }

    @Test
    void withArgs() throws IOException, SQLException {
        String url = "http://localhost:9191";
        main(new String[] {url}, url);
    }

    private void main(String[] args, String expectedHttpUrl) throws IOException, SQLException {
        String nativeUrl = "jdbc:h2:mem:test";
        assertThrows(SQLException.class, () -> DriverManager.getConnection(format("%s#%s", expectedHttpUrl, nativeUrl)));

        DriverController.main(args);
        Spark.awaitInitialization();

        Connection httpConn = DriverManager.getConnection(format("%s#%s", expectedHttpUrl, nativeUrl));
        assertNotNull(httpConn);
    }

    @AfterEach
    void afterAll() {
        Spark.stop();
        Spark.awaitStop();
    }
}

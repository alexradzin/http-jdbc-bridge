package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import spark.Spark;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class ControllerTestBase {
    protected static final String httpUrl = "http://localhost:8080";
    private static final Map<String, String> checkConnectionQuery =
            Stream.of(
                    new SimpleEntry<>("hsqldb", "call now()"),
                    new SimpleEntry<>("derby", "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
                    new SimpleEntry<>("db2", "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
                    new SimpleEntry<>("oracle", "SELECT 1 FROM DUAL")
            ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    @BeforeAll
    static void beforeAll() throws IOException {
        // Although this configuration is needed for DriverControllerTest.createAndCloseConnectionWithPredefinedUrl
        // it has to be done here because it uses static variables that are initialized in the beginning of the JVM life.
        System.setProperty("java.security.auth.login.config", "src/test/resources/jaas.conf");
        if (System.getProperty("jdbc.conf", System.getenv("jdbc.conf")) == null) {
            System.setProperty("jdbc.conf", "src/test/resources/jdbc.properties");
        }
        Spark.port(8080);
        new DriverController(new HashMap<>(), new ObjectMapper());
        Spark.awaitInitialization();
    }

    @AfterAll
    static void afterAll() {
        Spark.stop();
        Spark.awaitStop();
    }

    protected String db(String url) {
        return url.split(":")[1];
    }

    protected String getCheckConnectivityQuery(String db) {
        return checkConnectionQuery.getOrDefault(db, "select 1");
    }

}

package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import spark.Spark;

import java.util.HashMap;

abstract class ControllerTestBase {
    protected static final String httpUrl = "http://localhost:8080";

    @BeforeAll
    static void beforeAll() {
        Spark.port(8080);
        new DriverController(new HashMap<>(), new ObjectMapper());
        Spark.awaitInitialization();
    }

    @AfterAll
    static void afterAll() {
        Spark.stop();
        Spark.awaitStop();
    }
}

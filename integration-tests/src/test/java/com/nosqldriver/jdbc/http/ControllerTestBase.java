package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import net.sourceforge.htmlunit.corejs.javascript.Undefined;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import spark.Spark;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

abstract class ControllerTestBase {
    protected static final String httpUrl = "http://localhost:8080";
    private static final Map<String, String> checkConnectionQuery =
            Stream.of(
                    new SimpleEntry<>("hsqldb", "call now()"),
                    new SimpleEntry<>("derby", "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
                    new SimpleEntry<>("db2", "SELECT 1 FROM SYSIBM.SYSDUMMY1"),
                    new SimpleEntry<>("oracle", "SELECT 1 FROM DUAL")
            ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    protected String testName;
    protected final WebClient webClient = new WebClient();
    private final ObjectMapper objectMapper = new ObjectMapper();


    @BeforeAll
    static void beforeAll() throws IOException {
        // Although this configuration is needed for DriverControllerTest.createAndCloseConnectionWithPredefinedUrl
        // it has to be done here because it uses static variables that are initialized in the beginning of the JVM life.
        System.setProperty("java.security.auth.login.config", "src/test/resources/jaas.conf");
        if (System.getProperty("jdbc.conf", System.getenv("jdbc.conf")) == null) {
            System.setProperty("jdbc.conf", "src/test/resources/jdbc.properties");
        }
        Spark.staticFiles.location("/");
        Spark.port(8080);
        new DriverController(new HashMap<>(), new ObjectMapper());
        Spark.awaitInitialization();
    }


    @AfterAll
    static void afterAll() {
        Spark.stop();
        Spark.awaitStop();
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        testName = testInfo.getTestMethod().get().getName();
    }

    protected String db(String url) {
        return url.split(":")[1];
    }

    protected String getCheckConnectivityQuery(String db) {
        return checkConnectionQuery.getOrDefault(db, "select 1");
    }

    protected <T> T executeJavaScript(Object ... args) throws IOException {
        HtmlPage page = webClient.getPage(format("%s/tests.html", httpUrl));
        String argsStr = Arrays.stream(args)
                .map(a -> {
                    try {
                        return objectMapper.writeValueAsString(a).replace('"', '\'');
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.joining(", "));
        Object res = page.executeJavaScript(format("%s.%s(%s)", getClass().getSimpleName(), testName, argsStr)).getJavaScriptResult();
        if (res == null || res instanceof Undefined) {
            return null;
        }
        if (res instanceof Map) {
            new HashMap<>((Map<String, Object>)res);
        }
        return (T)res;
    }
}

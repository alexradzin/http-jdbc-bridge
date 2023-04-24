package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import com.nosqldriver.jdbc.http.json.ObjectMapperFactory;
import com.nosqldriver.util.function.ThrowingBiFunction;
import net.sourceforge.htmlunit.corejs.javascript.Undefined;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import spark.Spark;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    private static final ObjectMapper objectMapper = ObjectMapperFactory.createObjectMapper();
    protected static ThrowingBiFunction<String, String, String, SQLException> validator = (user, sql) -> sql;

    protected Connection nativeConn;
    protected Connection httpConn;

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
        new DriverController(new HashMap<>(), objectMapper, validator);
        Spark.awaitInitialization();
    }

    @AfterAll
    static void afterAll() throws IOException {
        Spark.stop();
        Spark.awaitStop();
    }

    @BeforeEach
    void beforeEach(TestInfo testInfo) {
        testName = testInfo.getTestMethod().get().getName();
    }

    @BeforeEach
    void initDb(TestInfo testInfo) throws SQLException {
        String nativeUrl = testInfo.getDisplayName();
        nativeConn = DriverManager.getConnection(nativeUrl);
        String url = format("%s#%s", httpUrl, nativeUrl);
        httpConn = DriverManager.getConnection(url);
    }

    @AfterEach
    void cleanDb() throws SQLException {
        nativeConn.close();
        httpConn.close();
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

    protected String sqlScript(String db, String file) {
        try {
            return new String(getClass().getResourceAsStream(format("/sql/%s/%s", db, file)).readAllBytes());
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}

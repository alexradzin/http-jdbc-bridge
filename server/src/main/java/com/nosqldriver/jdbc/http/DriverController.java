package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.json.ObjectMapperFactory;
import com.nosqldriver.jdbc.http.model.ConnectionInfo;
import com.nosqldriver.jdbc.http.model.ConnectionProxy;
import com.nosqldriver.jdbc.http.model.TransportableException;
import com.nosqldriver.jdbc.http.permissions.StatementPermissionsValidatorsConfigurer;
import com.nosqldriver.util.function.ThrowingBiFunction;
import spark.Spark;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.CredentialNotFoundException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static java.lang.String.format;
import static spark.Spark.options;
import static spark.Spark.post;

public class DriverController extends BaseController {
    private static final String JDBC_CONF_PROP = "jdbc.conf";
    private final Properties jdbcProps = new Properties();
    private String defaultDb;

    public DriverController(Map<String, Object> attributes, ObjectMapper objectMapper, ThrowingBiFunction<String, String, String, SQLException> validator) throws IOException {
        super(attributes, objectMapper);

        Spark.exception(Exception.class, (exception, request, response) -> {
            exception.printStackTrace();
            response.status(222);
            try {
                response.body(objectMapper.writeValueAsString(new TransportableException(exception)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
        });

        Spark.afterAfter((req, res) -> {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Headers", "content-type");
            res.header("Access-Control-Allow-Methods","PUT, POST, GET, DELETE, PATCH, OPTIONS");
        });

        options("*", (req, res) -> {
            res.status(HttpURLConnection.HTTP_NO_CONTENT);
            return "";
        });

        String authConfigProperty = System.getProperty("java.security.auth.login.config");
        if (authConfigProperty != null) {
            if (!new File(authConfigProperty).exists()) {
                throw new IllegalStateException(format("JAAS file %s is not found", authConfigProperty));
            }
        }
        boolean authRequired = authConfigProperty != null;

        post("/connection", JSON, (req, res) -> retrieve(() -> {
            ConnectionInfo connectionInfo = retrieveConnectionInfo(objectMapper.readValue(req.bodyAsBytes(), ConnectionInfo.class), authRequired);
            String configuredUrl = connectionInfo.getUrl();
            String jdbcUrl = configuredUrl;
            if (!configuredUrl.startsWith("jdbc:")) {
                jdbcUrl = jdbcProps.getProperty(configuredUrl);
            }
            Properties connectionProperties = null;
            if (jdbcUrl.endsWith("#properties")) {
                jdbcUrl = jdbcUrl.substring(0, jdbcUrl.length() - "#properties".length());
                connectionProperties = connectionInfo.getProperties();
            }
            Connection connection = DriverManager.getConnection(jdbcUrl, connectionProperties);
            setAttribute("connection-info", System.identityHashCode(connection), connectionInfo);
            return connection;
        }, connection -> connection, ConnectionProxy::new, "connection", req.url()));

        post("/acceptsurl", JSON, (req, res) -> retrieve(() -> {
            String url = objectMapper.readValue(req.body(), String.class);
            String[] parts = url.split("#", 2);
            if (parts.length < 2) {
                return true;
            }
            String fragment = parts[1];
            String jdbcUrl = fragment.startsWith("jdbc:") ? fragment : jdbcProps.getProperty(fragment);
            if (jdbcUrl == null) {
                return false;
            }
            for(Enumeration<Driver> ed = DriverManager.getDrivers(); ed.hasMoreElements();) {
                Driver driver = ed.nextElement();
                if (driver.acceptsURL(jdbcUrl)) {
                    return true;
                }
            }
            return false;
        }, accepts -> accepts));

        new ConnectionController(attributes, objectMapper, validator);
        File jdbcConf = getConfigurationFile(JDBC_CONF_PROP, "jdbc.properties");
        if (jdbcConf.exists()) {
            jdbcProps.load(new FileReader(jdbcConf));
            defaultDb = jdbcProps.getProperty("default", System.getProperty("default_db", System.getenv("default_db")));
        }
    }

    private ConnectionInfo retrieveConnectionInfo(ConnectionInfo clientConnectionInfo, boolean authRequired) throws LoginException {
        authenticate(clientConnectionInfo.getProperties(), authRequired);
        String url = clientConnectionInfo.getUrl();
        if (url != null) {
            return clientConnectionInfo;
        }
        String user = clientConnectionInfo.getProperties().getProperty("user");
        String jdbcUrl = jdbcProps.getProperty(user, defaultDb);
        if (jdbcUrl == null) {
            throw new LoginException(format("User %s is not mapped to any JDBC URL", user));
        }
        return new ConnectionInfo(jdbcUrl, clientConnectionInfo.getProperties());
    }

    private void authenticate(Properties props, boolean authRequired) throws LoginException {
        authenticate(props.getProperty("user"), props.getProperty("password"), authRequired);
    }

    private void authenticate(String user, String password, boolean authRequired) throws LoginException {
        if (user == null || password == null || "".equals(user) || "".equals(password)) {
            if (authRequired) {
                throw new CredentialNotFoundException("User and password are required to login");
            }
            return;
        }
        LoginContext lc = new LoginContext("HttpJdbcBridge", callbacks -> {
            for (Callback cb : callbacks) {
                if (cb instanceof NameCallback) {
                    ((NameCallback)cb).setName(user);
                }
                if (cb instanceof PasswordCallback) {
                    ((PasswordCallback)cb).setPassword(password.toCharArray());
                }
            }
        });
        lc.login();
    }

    private static File getConfigurationFile(String propertyName, String defaultName) {
        return new File(Optional.ofNullable(System.getProperty(propertyName, System.getenv(propertyName))).orElse(defaultName));
    }

    public static void main(String[] args) throws IOException {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080/";
        int port = new URL(baseUrl).getPort();
        Spark.staticFiles.location("/");
        if (port > 0) {
            spark.Spark.port(new URL(baseUrl).getPort());
        }
        ThrowingBiFunction<String, String, String, SQLException> validator = new StatementPermissionsValidatorsConfigurer().config();
        new DriverController(new HashMap<>(), ObjectMapperFactory.createObjectMapper(), validator);
        System.out.println("ready");
    }
}

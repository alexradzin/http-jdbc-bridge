package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ConnectionInfo;
import com.nosqldriver.jdbc.http.model.ConnectionProxy;
import com.nosqldriver.jdbc.http.model.TransportableException;
import spark.Spark;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Driver;
import java.sql.DriverManager;
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

    public DriverController(Map<String, Object> attributes, ObjectMapper objectMapper) throws IOException {
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


        post("/connection", JSON, (req, res) -> retrieve(() -> {
            ConnectionInfo connectionInfo = retrieveConnectionInfo(objectMapper.readValue(req.bodyAsBytes(), ConnectionInfo.class));
            return DriverManager.getConnection(connectionInfo.getUrl(), connectionInfo.getProperties());
        }, connection -> connection, ConnectionProxy::new, "connection", req.url()));

        post("/acceptsurl", JSON, (req, res) -> retrieve(() -> {
            String url = req.body();
            String[] parts = url.split("#", 2);
            if (parts.length < 2) {
                return true;
            }
            String jdbcUrl = parts[1];

            for(Enumeration<Driver> ed = DriverManager.getDrivers(); ed.hasMoreElements();) {
                Driver driver = ed.nextElement();
                if (driver.acceptsURL(jdbcUrl)) {
                    return true;
                }
            }
            return false;
        }, accepts -> accepts));

        new ConnectionController(attributes, objectMapper);
        File jdbcConf = new File(Optional.ofNullable(System.getProperty(JDBC_CONF_PROP, System.getenv(JDBC_CONF_PROP))).orElse("jdbc.properties"));
        if (jdbcConf.exists()) {
            jdbcProps.load(new FileReader(jdbcConf));
        }
    }

    private ConnectionInfo retrieveConnectionInfo(ConnectionInfo clientConnectionInfo) throws LoginException {
        String url = clientConnectionInfo.getUrl();
        if (url != null) {
            return clientConnectionInfo;
        }
        authenticate(clientConnectionInfo.getProperties());
        String user = clientConnectionInfo.getProperties().getProperty("user");
        String jdbcUrl = jdbcProps.getProperty(user);
        if (jdbcUrl == null) {
            throw new LoginException(format("User %s is not mapped to any JDBC URL", user));
        }
        return new ConnectionInfo(jdbcUrl, new Properties());
    }

    private void authenticate(Properties props) throws LoginException {
        authenticate(props.getProperty("user"), props.getProperty("password"));
    }

    private void authenticate(String user, String password) throws LoginException {
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


    public static void main(String[] args) throws IOException {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080/";
        int port = new URL(baseUrl).getPort();
        Spark.staticFiles.location("/");
        if (port > 0) {
            spark.Spark.port(new URL(baseUrl).getPort());
        }

        Map<String, Object> attributes = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        new DriverController(attributes, objectMapper);
        System.out.println("ready");
    }
}

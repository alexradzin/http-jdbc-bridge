package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ConnectionInfo;
import com.nosqldriver.jdbc.http.model.ConnectionProxy;
import com.nosqldriver.jdbc.http.model.TransportableException;
import spark.Spark;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import static spark.Spark.post;

public class DriverController extends BaseController {
    public DriverController(Map<String, Object> attributes, ObjectMapper objectMapper) {
        super(attributes, objectMapper);

        Spark.exception(Exception.class, (exception, request, response) -> {
            exception.printStackTrace();
            response.status(222);
            try {
                response.body(objectMapper.writeValueAsString(new TransportableException(exception)));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        });

        post("/connection", JSON, (req, res) -> retrieve(() -> {
            ConnectionInfo connectionInfo = objectMapper.readValue(req.bodyAsBytes(), ConnectionInfo.class);
            return DriverManager.getConnection(connectionInfo.getUrl(), connectionInfo.getProperties());
        }, connection -> connection, ConnectionProxy::new, "connection", req.url()));

        post("/acceptsurl", JSON, (req, res) -> retrieve(() -> {
            String url = req.body();
            String jdbcUrl = url.split("#", 2)[1];

            for(Enumeration<Driver> ed = DriverManager.getDrivers(); ed.hasMoreElements();) {
                Driver driver = ed.nextElement();
                if (driver.acceptsURL(jdbcUrl)) {
                    return true;
                }
            }
            return false;
        }, accepts -> accepts));

        new ConnectionController(attributes, objectMapper);
    }

    public static void main(String[] args) throws MalformedURLException {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080/";
        int port = new URL(baseUrl).getPort();
        if (port > 0) {
            spark.Spark.port(new URL(baseUrl).getPort());
        }
        Map<String, Object> attributes = new HashMap<>();
        ObjectMapper objectMapper = new ObjectMapper();
        new DriverController(attributes, objectMapper);
        System.out.println("ready");
    }
}

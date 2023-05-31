package com.nosqldriver.jdbc.http;

import com.nosqldriver.jdbc.http.json.ObjectMapperFactory;
import com.nosqldriver.jdbc.http.permissions.StatementPermissionsValidatorsConfigurer;
import com.nosqldriver.util.function.ThrowingBiFunction;
import spark.Spark;

import java.io.Closeable;
import java.io.IOException;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;

public class Server {
    public static void main(String[] args) throws IOException {
        String baseUrl = args.length > 0 ? args[0] : "http://localhost:8080/";
        int port = new URL(baseUrl).getPort();
        Spark.staticFiles.location("/");
        if (port > 0) {
            spark.Spark.port(new URL(baseUrl).getPort());
        }

        StatementPermissionsValidatorsConfigurer configurer = new StatementPermissionsValidatorsConfigurer();
        ThrowingBiFunction<String, String, String, SQLException> validator = configurer.config();
        Closeable driverController = new DriverController(new HashMap<>(), ObjectMapperFactory.createObjectMapper(), validator, configurer);
        Spark.awaitInitialization();
        System.out.println("ready");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                driverController.close();
            } catch (IOException e) {
                e.printStackTrace(); // TODO: add logging? Although does it really matter to log exception thrown from a shutdown hook?
            }
        }));
    }
}

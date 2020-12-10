package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.io.Closeable;
import java.util.Map;

import static spark.Spark.delete;

public class ClosableController extends BaseController {
    private final String type;

    protected ClosableController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl, String type) {
        super(attributes, objectMapper);
        this.type = type;
        delete(baseUrl, JSON, (req, res) -> accept(() -> getCloseable(attributes, req), Closeable::close));
    }

    private Closeable getCloseable(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, type, ":" + type);
    }
}

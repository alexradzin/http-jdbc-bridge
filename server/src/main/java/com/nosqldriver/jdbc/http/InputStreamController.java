package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.io.InputStream;
import java.util.Map;

import static spark.Spark.get;

public class InputStreamController extends BaseController {
    protected InputStreamController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        get(baseUrl, JSON, (req, res) -> retrieve(() -> getInputStream(attributes, req), InputStream::read));
    }

    private InputStream getInputStream(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "stream", ":stream");
    }
}

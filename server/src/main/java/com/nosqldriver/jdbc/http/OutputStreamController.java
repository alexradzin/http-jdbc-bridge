package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.io.OutputStream;
import java.util.Map;

import static spark.Spark.delete;
import static spark.Spark.post;
import static spark.Spark.put;

public class OutputStreamController extends BaseController {
    protected OutputStreamController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        put(baseUrl, JSON, (req, res) -> accept(() -> getOutputStream(attributes, req), os -> os.write(objectMapper.readValue(req.body(), int.class))));
        post(baseUrl, JSON, (req, res) -> accept(() -> getOutputStream(attributes, req), OutputStream::flush));
        delete(baseUrl, JSON, (req, res) -> accept(() -> getOutputStream(attributes, req), OutputStream::close));
    }

    private OutputStream getOutputStream(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "stream", ":stream");
    }
}

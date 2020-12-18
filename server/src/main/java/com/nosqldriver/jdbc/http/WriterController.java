package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.io.Writer;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.post;
import static spark.Spark.put;

public class WriterController extends BaseController {
    protected WriterController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        put(format("%s/:off/:len", baseUrl), JSON, (req, res) -> accept(() -> getWriter(attributes, req), writer -> writer.write(objectMapper.readValue(req.bodyAsBytes(), char[].class), intParam(req, ":off"), intParam(req, ":len"))));
        post(baseUrl + "/flush", JSON, (req, res) -> accept(() -> getWriter(attributes, req), Writer::flush));
    }

    private Writer getWriter(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "stream", ":stream");
    }
}

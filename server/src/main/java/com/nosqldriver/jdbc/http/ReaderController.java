package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.util.function.ThrowingFunction;
import spark.Request;
import spark.Response;
import spark.Route;

import java.io.Reader;
import java.sql.Connection;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.get;
import static spark.Spark.delete;

public class ReaderController extends BaseController {
    protected ReaderController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        get(format("%s/:off/:len", baseUrl), JSON, (req, res) -> retrieve(() -> getReader(attributes, req), (ThrowingFunction<Reader, Object, Exception>) reader -> {
            int length = intParam(req, ":len");
            char[] buf = new char[length];
            int actual = reader.read(buf, intParam(req, ":off"), intParam(req, ":len"));
            if (actual < length) {
                char[] truncated = new char[actual];
                System.arraycopy(buf, 0, truncated, 0, actual);
                return truncated;
            }
            return buf;
        }));

        delete(baseUrl, JSON, (req, res) -> accept(() -> getReader(attributes, req), Reader::close));
    }

    private Reader getReader(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "reader", ":reader");
    }
}

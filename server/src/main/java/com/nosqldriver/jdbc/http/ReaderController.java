package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.util.function.ThrowingFunction;
import spark.Request;

import java.io.Reader;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.get;

public class ReaderController extends BaseController {
    protected ReaderController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        get(format("%s/:off/:len", baseUrl), JSON, (req, res) -> retrieve(() -> getReader(attributes, req), (ThrowingFunction<Reader, Object, Exception>) reader -> {
            int length = intParam(req, ":len");
            char[] buf = new char[length];
            int actual = reader.read(buf, intParam(req, ":off"), intParam(req, ":len"));
            if (actual < length) {
                if (actual < 0) {
                    return null;
                }
                char[] truncated = new char[actual];
                System.arraycopy(buf, 0, truncated, 0, actual);
                return truncated;
            }
            return buf;
        }));
    }

    private Reader getReader(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "stream", ":stream");
    }
}

package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.sql.Clob;
import java.sql.Struct;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.post;

public class StructController extends BaseController {
    protected StructController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        post(format("%s/attributes", baseUrl), JSON, (req, res) -> retrieve(() -> getStruct(attributes, req), b -> b.getAttributes(objectMapper.readValue(req.body(), Map.class))));
    }

    private Struct getStruct(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "struct", ":struct");
    }
}



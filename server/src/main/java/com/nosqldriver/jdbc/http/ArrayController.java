package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.sql.Array;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

public class ArrayController extends BaseController {
    protected ArrayController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);

        get(format("%s/array", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), Array::getArray));
        get(format("%s/array/:index/:count", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), a -> a.getArray(intParam(req, ":index"), intParam(req, ":count"))));
        post(format("%s/array", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), a -> a.getArray(objectMapper.readValue(req.body(), Map.class))));
        post(format("%s/array/:index/:count", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), a -> a.getArray(intParam(req, ":index"), intParam(req, ":count"), objectMapper.readValue(req.body(), Map.class))));

        get(format("%s/resultset", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), Array::getResultSet));
        get(format("%s/resultset/:index/:count", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), a -> a.getResultSet(intParam(req, ":index"), intParam(req, ":count"))));
        post(format("%s/resultset", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), a -> a.getResultSet(objectMapper.readValue(req.body(), Map.class))));
        post(format("%s/resultset/:index/:count", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), a -> a.getResultSet(intParam(req, ":index"), intParam(req, ":count"), objectMapper.readValue(req.body(), Map.class))));

        delete(baseUrl, JSON, (req, res) -> accept(() -> getArray(attributes, req), Array::free));

        new ResultSetController(attributes, objectMapper, baseUrl + "/resultset/*", false);
    }


    private Array getArray(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "array", ":array");
    }
}

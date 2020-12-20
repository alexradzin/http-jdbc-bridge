package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ResultSetProxy;
import com.nosqldriver.util.function.ThrowingFunction;
import spark.Request;

import java.sql.Array;
import java.sql.ResultSet;
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
        get(format("%s/basetype", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), Array::getBaseType));
        get(format("%s/basetypename", baseUrl), JSON, (req, res) -> retrieve(() -> getArray(attributes, req), Array::getBaseTypeName));

        get(format("%s/resultset", baseUrl), JSON, (req, res) -> {
            Long index = longArg(req, "index");
            Integer count = intArg(req, "count");
            ThrowingFunction<Array, ResultSet, Exception> f = index != null && count != null ? a -> a.getResultSet(index, count) : Array::getResultSet;
            return retrieve(() -> getArray(attributes, req), f, ResultSetProxy::new, "resultset", req.url());
        });

        post(format("%s/resultset", baseUrl), JSON, (req, res) -> {
            Long index = longArg(req, "index");
            Integer count = intArg(req, "count");
            @SuppressWarnings("unchecked")
            Map<String, Class<?>> map = objectMapper.readValue(req.body(), Map.class);
            ThrowingFunction<Array, ResultSet, Exception> f = index != null && count != null ? a -> a.getResultSet(index, count, map) : a -> a.getResultSet(map);
            return retrieve(() -> getArray(attributes, req), f, ResultSetProxy::new, "resultset", req.url());
        });

        delete(baseUrl, JSON, (req, res) -> accept(() -> getArray(attributes, req), Array::free));

        new ResultSetController(attributes, objectMapper, baseUrl + "/resultset/:resultset", false);
    }


    private Array getArray(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "array", ":array");
    }
}

package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.InputStreamProxy;
import com.nosqldriver.jdbc.http.model.ReaderProxy;
import com.nosqldriver.jdbc.http.model.WriterProxy;
import spark.Request;

import java.sql.Clob;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

public class ClobController extends BaseController {
    protected ClobController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        get(format("%s/substring/:pos/:length", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), b -> b.getSubString(intParam(req, ":pos"), intParam(req, ":length"))));
        get(format("%s/ascii/stream", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), Clob::getAsciiStream, InputStreamProxy::new, "stream", req.url()));
        get(format("%s/character/stream", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), Clob::getCharacterStream, ReaderProxy::new, "reader", req.url()));
        get(format("%s/character/stream/:pos/:length", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), c -> c.getCharacterStream(intParam(req, ":pos"), intParam(req, ":length")), ReaderProxy::new, "reader", req.url()));
        post(format("%s/position/:start", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), b -> b.position(req.body(), intParam(req, ":start"))));
        post(format("%s/:pos", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), b -> b.setString(intParam(req, ":pos"), objectMapper.readValue(req.body(), String.class))));

//        post(format("%s/:pos/:offset/:length", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), b -> b.setString(intParam(req, ":pos"), objectMapper.readValue(req.body(), String.class), intParam(req, ":offset"), intParam(req, ":length"))));
//        post(format("%s/ascii/stream/:pos", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), c -> c.setAsciiStream(intParam(req, ":pos")), InputStreamProxy::new, "stream", req.url()));

        post(format("%s/:one/:two/:pos", baseUrl), JSON, (req, res) -> {
            String type = stringParam(req, ":one");
            if ("ascii".equals(type)) {
                return retrieve(() -> getClob(attributes, req), c -> c.setAsciiStream(intParam(req, ":pos")), InputStreamProxy::new, "stream", parentUrl(req.url()));
            }
            return retrieve(() -> getClob(attributes, req), b -> b.setString(intParam(req, ":one"), objectMapper.readValue(req.body(), String.class), intParam(req, ":two"), intParam(req, ":length")));
        });


        post(format("%s/character/stream/:pos", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), c -> c.setCharacterStream(intParam(req, ":pos")), WriterProxy::new, "writer", req.url()));
        delete(baseUrl, JSON, (req, res) -> accept(() -> getClob(attributes, req), b -> {
            Long len = longArg(req, ":len");
            if (len == null) {
                b.free();
            } else {
                b.truncate(len);
            }
        }));

        new ReaderController(attributes, objectMapper, baseUrl + "/character/stream/:stream");
        new WriterController(attributes, objectMapper, baseUrl + "/character/stream/:stream");
        new InputStreamController(attributes, objectMapper, baseUrl + "/ascii/stream/:stream");
        new OutputStreamController(attributes, objectMapper, baseUrl + "/ascii/stream/:stream");
    }

    private Clob getClob(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "clob", ":clob");
    }
}

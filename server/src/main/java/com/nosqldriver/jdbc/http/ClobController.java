package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.nosqldriver.jdbc.http.model.BlobProxy;
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
        get(format("%s/character/stream", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), Clob::getCharacterStream, ReaderProxy::new, "stream", req.url()));
        get(format("%s/character/stream/:pos/:length", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), c -> c.getCharacterStream(intParam(req, ":pos"), intParam(req, ":length")), ReaderProxy::new, "stream", parentUrl(parentUrl(req.url()))));

        post(format("%s/position/:start", baseUrl), JSON, (req, res) -> {
            byte[] content = req.bodyAsBytes();
            JsonNode node = objectMapper.readTree(content);
            if (JsonNodeType.OBJECT.equals(node.getNodeType())) {
                String[] refUrlParts = objectMapper.readValue(content, BlobProxy.class).getEntityUrl().split("/");
                String id = refUrlParts[refUrlParts.length - 1];
                Clob pattern = getEntity(attributes, "clob", id);
                return retrieve(() -> getClob(attributes, req), b -> b.position(pattern, intParam(req, ":start")));
            }
            return retrieve(() -> getClob(attributes, req), b -> b.position(objectMapper.readValue(req.body(), String.class), intParam(req, ":start")));
        });

        post(format("%s/:pos", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), b -> b.setString(intParam(req, ":pos"), objectMapper.readValue(req.body(), String.class))));
        get(format("%s/length", baseUrl), JSON, (req, res) -> retrieve(() -> getClob(attributes, req), Clob::length));

        post(format("%s/:one/:two/:three", baseUrl), JSON, (req, res) -> {
            String type = stringParam(req, ":one");
            if ("ascii".equals(type)) {
                return retrieve(() -> getClob(attributes, req), c -> c.setAsciiStream(intParam(req, ":three")), InputStreamProxy::new, "stream", parentUrl(req.url()));
            }
            if ("character".equals(type)) {
                return retrieve(() -> getClob(attributes, req), c -> c.setCharacterStream(intParam(req, ":three")), WriterProxy::new, "stream", parentUrl(req.url()));
            }
            return retrieve(() -> getClob(attributes, req), b -> b.setString(intParam(req, ":one"), objectMapper.readValue(req.body(), String.class), intParam(req, ":two"), intParam(req, ":three")));
        });

        delete(baseUrl, JSON, (req, res) -> accept(() -> getClob(attributes, req), b -> {
            Long len = objectMapper.readValue(req.bodyAsBytes(), Long.class);
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

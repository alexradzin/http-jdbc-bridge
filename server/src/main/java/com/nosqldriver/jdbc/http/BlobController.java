package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.OutputStreamProxy;
import spark.Request;

import java.sql.Blob;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

public class BlobController extends BaseController {
    protected BlobController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);

        get(format("%s/bytes/:pos/:length", baseUrl), JSON, (req, res) -> retrieve(() -> getBlob(attributes, req), b -> b.getBytes(intParam(req, ":pos"), intParam(req, ":length"))));
        post(format("%s/position/:pos/:length", baseUrl), JSON, (req, res) -> retrieve(() -> getBlob(attributes, req), b -> b.position(req.bodyAsBytes(), intParam(req, ":length"))));
        get(format("%s/binary/stream", baseUrl), JSON, (req, res) -> getBlob(attributes, req).getBinaryStream());
        get(format("%s/binary/stream/:pos/:len", baseUrl), JSON, (req, res) -> getBlob(attributes, req).getBinaryStream(longParam(req, ":pos"), intParam(req, ":len")));
        post(format("%s/bytes/:pos", baseUrl), JSON, (req, res) -> retrieve(() -> getBlob(attributes, req), b -> b.setBytes(intParam(req, ":pos"), objectMapper.readValue(req.bodyAsBytes(), byte[].class))));
        post(format("%s/bytes/:offset/:len", baseUrl), JSON, (req, res) -> retrieve(() -> getBlob(attributes, req), b -> b.setBytes(intParam(req, ":pos"), objectMapper.readValue(req.bodyAsBytes(), byte[].class))));
        post(format("%s/binary/stream/:pos", baseUrl), JSON, (req, res) -> retrieve(() -> getBlob(attributes, req), c -> c.setBinaryStream(intParam(req, ":pos")), OutputStreamProxy::new, "stream", parentUrl(req.url())));
        delete(baseUrl, JSON, (req, res) -> accept(() -> getBlob(attributes, req), b -> {
            Long len = longArg(req, ":len");
            if (len == null) {
                b.free();
            } else {
                b.truncate(len);
            }
        }));

        new InputStreamController(attributes, objectMapper, baseUrl + "/binary/stream/:stream");
        new OutputStreamController(attributes, objectMapper, baseUrl + "/binary/stream/:stream");
    }

    private Blob getBlob(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "blob", ":blob");
    }
}

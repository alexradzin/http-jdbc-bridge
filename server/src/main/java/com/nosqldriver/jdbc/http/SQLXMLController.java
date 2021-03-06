package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.InputStreamProxy;
import com.nosqldriver.jdbc.http.model.OutputStreamProxy;
import com.nosqldriver.jdbc.http.model.ReaderProxy;
import com.nosqldriver.jdbc.http.model.WriterProxy;
import spark.Request;

import java.sql.SQLXML;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

public class SQLXMLController extends BaseController {
    protected SQLXMLController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);

        delete(baseUrl, JSON, (req, res) -> accept(() -> getSQLXML(attributes, req), SQLXML::free));
        get(format("%s/binary/stream", baseUrl), JSON, (req, res) -> retrieve(() -> getSQLXML(attributes, req), SQLXML::getBinaryStream, InputStreamProxy::new, "stream", req.url()));
        post(format("%s/binary/stream", baseUrl), JSON, (req, res) -> retrieve(() -> getSQLXML(attributes, req), SQLXML::setBinaryStream, OutputStreamProxy::new, "stream", req.url()));
        get(format("%s/character/stream", baseUrl), JSON, (req, res) -> retrieve(() -> getSQLXML(attributes, req), SQLXML::getCharacterStream, ReaderProxy::new, "stream", req.url()));
        post(format("%s/character/stream", baseUrl), JSON, (req, res) -> retrieve(() -> getSQLXML(attributes, req), SQLXML::setCharacterStream, WriterProxy::new, "stream", req.url()));
        get(format("%s/string", baseUrl), JSON, (req, res) -> retrieve(() -> getSQLXML(attributes, req), SQLXML::getString));
        post(format("%s/string", baseUrl), JSON, (req, res) -> accept(() -> getSQLXML(attributes, req), sqlxml -> sqlxml.setString(objectMapper.readValue(req.body(), String.class))));
        get(format("%s/source/:class", baseUrl), JSON, (req, res) -> retrieve(() -> getSQLXML(attributes, req), sqlxml -> sqlxml.getSource((Class)Class.forName(stringParam(req, ":class")))));
        post(format("%s/source", baseUrl), JSON, (req, res) -> accept(() -> getSQLXML(attributes, req), sqlxml -> sqlxml.getSource((Class)Class.forName(objectMapper.readValue(req.body(), String.class)))));

        String readWriterFormat = "/connection/:connection/sqlxml/:sqlxml/%s/stream/:stream";
        String characterStreamPath = format(readWriterFormat, "character");
        String binaryStreamPath = format(readWriterFormat, "binary");
        new WriterController(attributes, objectMapper, characterStreamPath);
        new ReaderController(attributes, objectMapper, characterStreamPath);
        new OutputStreamController(attributes, objectMapper, binaryStreamPath);
        new InputStreamController(attributes, objectMapper, binaryStreamPath);
    }

    private SQLXML getSQLXML(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "sqlxml", ":sqlxml");
    }
}

package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import spark.Request;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static spark.Spark.delete;

abstract class AutoClosableController extends BaseController {
    private final String prefix;
    private final String id;
    private final List<String> subCloseableIds = new LinkedList<>();

    protected AutoClosableController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        String[] urlParts =  baseUrl.split("/");
        prefix = urlParts[urlParts.length - 2];
        id = urlParts[urlParts.length - 1];
        delete(baseUrl, JSON, (req, res) -> accept(() -> getCloseable(attributes, req), this::close));
    }

    @Override
    protected <T> void setAttribute(String key, T entity) {
        super.setAttribute(key, entity);
        subCloseableIds.add(key);
    }

    protected Entry<String, AutoCloseable> getCloseable(Map<String, Object> attributes, Request req) {
        String rsId = getEntityId(req, prefix, id);
        AutoCloseable closeable = getAttribute(attributes, rsId);
        return closeable == null ? null : Map.entry(rsId, getAttribute(attributes, rsId));
    }

    private void close(Entry<String, AutoCloseable> closeableEntry) throws Exception {
        if (closeableEntry != null) {
            close(closeableEntry.getKey(), closeableEntry.getValue());
        }
    }

    private void close(String id, AutoCloseable closable) throws Exception {
        closable.close();
        attributes.remove(id);

        subCloseableIds.forEach(attributes::remove);
        subCloseableIds.clear();
    }
}

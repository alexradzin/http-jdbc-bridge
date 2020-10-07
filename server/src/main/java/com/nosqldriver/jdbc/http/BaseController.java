package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import com.nosqldriver.util.function.ThrowingSupplier;
import spark.Request;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static java.lang.String.format;

abstract class BaseController {
    protected final String JSON = "application/json";
    private final Map<String, Object> attributes;
    protected final ObjectMapper objectMapper;

    protected BaseController(Map<String, Object> attributes, ObjectMapper objectMapper) {
        this.attributes = attributes;
        this.objectMapper = objectMapper;
    }

//    protected <T> String retrieve(ThrowingSupplier<Connection, Exception> connectionSupplier, ThrowingFunction<Connection, T, Exception> entityFactory, ThrowingFunction<String, T, Exception> proxyFactory, String prefix, String url) throws Exception {
//        return retrieve2(connectionSupplier, entityFactory, (url1, entity) -> proxyFactory.apply(url1), prefix, url);
//    }
//
//    protected <T> String retrieve2(ThrowingSupplier<Connection, Exception> connectionSupplier, ThrowingFunction<Connection, T, Exception> entityFactory, ThrowingBiFunction<String, T, T, Exception> proxyFactory, String prefix, String url) throws Exception {
//        Connection connection = connectionSupplier.get();
//        T entity = entityFactory.apply(connection);
//        int entityId = System.identityHashCode(entity);
//        attributes.put(prefix + "@" + entityId, connection);
//        return objectMapper.writeValueAsString(proxyFactory.apply(format("%s/%d", url, entityId), entity));
//    }

    protected <P, T> String retrieve(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory, ThrowingFunction<String, T, Exception> proxyFactory, String prefix, String url) throws Exception {
        return retrieve2(parentSupplier, entityFactory, (url1, entity) -> proxyFactory.apply(url1), prefix, url);
    }

    protected <P, T> String retrieve2(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory, ThrowingBiFunction<String, T, T, Exception> proxyFactory, String prefix, String url) throws Exception {
        P parent = parentSupplier.get();
        T entity = entityFactory.apply(parent);
        if (entity == null) {
            return null;
        }
        int entityId = System.identityHashCode(entity);
        attributes.put(prefix + "@" + entityId, entity);
        return objectMapper.writeValueAsString(proxyFactory.apply(format("%s/%s/%d", parentUrl(url), prefix, entityId), entity));
    }

    private String parentUrl(String url) {
        String parent = url.replaceFirst("/$", "");
        int lastSlash = parent.lastIndexOf('/');
        if (lastSlash >= 0) {
            parent = parent.substring(0, lastSlash);
        }
        return parent;
    }

    protected <P, T> String retrieve(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory, ThrowingFunction<T, T, Exception> transportableEntityFactory) throws Exception {
        return objectMapper.writeValueAsString(transportableEntityFactory.apply(entityFactory.apply(parentSupplier.get())));
    }

    protected <P, T> String retrieve(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory) throws Exception {
        return objectMapper.writeValueAsString(entityFactory.apply(parentSupplier.get()));
    }

    protected <P> Object accept(ThrowingSupplier<P, Exception> parentSupplier, ThrowingConsumer<P, Exception> entityFactory) throws Exception {
        entityFactory.accept(parentSupplier.get());
        return "";
    }


    @SuppressWarnings("unchecked")
    protected <T> T getEntity(Map<String, Object> attributes, Request req, String prefix, String id) {
        return (T)attributes.get(format("%s@%s", prefix, req.params(id)));
    }

    protected int intParam(Request req, String name) {
        return intValue(req.params(name), name);
    }

    protected String stringParam(Request req, String name) throws UnsupportedEncodingException {
        return stringValue(req.params(name), name);
    }

    protected int[] intArrayParam(Request req, String name) {
        return intArrayValue(req.params(name), name);
    }

    protected String[] stringArrayParam(Request req, String name) {
        return stringArrayValue(req.params(name), name);
    }

    ///////////////////
    protected int intArg(Request req, String name) {
        return intValue(req.queryParams(name), name);
    }

    protected String stringArg(Request req, String name) throws UnsupportedEncodingException {
        return stringValue(req.queryParams(name), name);
    }

    protected int[] intArrayArg(Request req, String name) {
        return intArrayValue(req.queryParams(name), name);
    }

    protected String[] stringArrayArg(Request req, String name) {
        return stringArrayValue(req.queryParams(name), name);
    }

    //////////////////

    protected int intValue(String str, String name) {
        return Integer.parseInt(str);
    }

    protected String stringValue(String value, String name) throws UnsupportedEncodingException {
        return value == null ? null : URLDecoder.decode(value, StandardCharsets.UTF_8.name());
    }

    protected int[] intArrayValue(String str, String name) {
        if (str == null) {
            return null;
        }
        if ("".equals(str)) {
            return new int[0];
        }
        return Arrays.stream(str.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    protected String[] stringArrayValue(String str, String name) {
        if (str == null) {
            return null;
        }
        if ("".equals(str)) {
            return new String[0];
        }
        return str.split(",");
    }


}

package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ConnectionProperties;
import com.nosqldriver.jdbc.http.model.ResultSetProxy;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import com.nosqldriver.util.function.ThrowingSupplier;
import spark.Request;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;

abstract class BaseController {
    protected final String JSON = "application/json";
    private final Map<String, Object> attributes;
    protected final ObjectMapper objectMapper;
    private final Map<String, ConnectionProperties> connectionPropertiesCache = new ConcurrentHashMap<>();
    private static final ConnectionProperties defaultConnectionProperties = new ConnectionProperties(System.getProperties());

    protected BaseController(Map<String, Object> attributes, ObjectMapper objectMapper) {
        this.attributes = attributes;
        this.objectMapper = objectMapper;
    }

    protected <P, T> String retrieve(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory, ThrowingFunction<String, T, Exception> proxyFactory, String prefix, String url) throws Exception {
        return retrieve2(parentSupplier, entityFactory, (url1, entity) -> proxyFactory.apply(url1), prefix, url);
    }

    protected <P, T> String retrieve2(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory, ThrowingBiFunction<String, T, T, Exception> proxyFactory, String prefix, String url) throws Exception {
        P parent = parentSupplier.get();
        T entity = entityFactory.apply(parent);
        if (entity == null) {
            return "null";
        }
        return objectMapper.writeValueAsString(entityToProxy(entity, proxyFactory, prefix, url));
    }

    protected <T> T entityToProxy(T entity, ThrowingBiFunction<String, T, T, Exception> proxyFactory, String prefix, String url) throws Exception {
        int entityId = System.identityHashCode(entity);
        attributes.put(prefix + "@" + entityId, entity);
        return proxyFactory.apply(format("%s/%s/%d", parentUrl(url), prefix, entityId), entity);
    }

    protected String parentUrl(String url) {
        String parent = url.replaceFirst("/$", "");
        int lastSlash = parent.lastIndexOf('/');
        if (lastSlash >= 0) {
            parent = parent.substring(0, lastSlash);
        }
        return parent;
    }

    protected <P, T> String retrieve(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory, ThrowingFunction<T, T, Exception> transportableEntityFactory) throws Exception {
        T entity = entityFactory.apply(parentSupplier.get());
        return objectMapper.writeValueAsString(entity == null ? null : transportableEntityFactory.apply(entity));
    }

    protected <P, T> String retrieve(ThrowingSupplier<P, Exception> parentSupplier, ThrowingFunction<P, T, Exception> entityFactory) throws Exception {
        T entity = entityFactory.apply(parentSupplier.get());
        return objectMapper.writeValueAsString(entity);
    }

    protected <P> Object accept(ThrowingSupplier<P, Exception> parentSupplier, ThrowingConsumer<P, Exception> entityFactory) throws Exception {
        entityFactory.accept(parentSupplier.get());
        return "null";
    }


    protected <T> T getEntity(Map<String, Object> attributes, Request req, String prefix, String idName) {
        return getEntity(attributes, prefix, req.params(idName));
    }

    @SuppressWarnings("unchecked")
    protected <T> T getEntity(Map<String, Object> attributes, String prefix, String id) {
        return (T)attributes.get(format("%s@%s", prefix, id));
    }

    protected int intParam(Request req, String name) {
        return intValue(req.params(name));
    }

    protected long longParam(Request req, String name) {
        return longValue(req.params(name));
    }

    protected String stringParam(Request req, String name) throws UnsupportedEncodingException {
        return req.params(name);
    }

    protected Integer intArg(Request req, String name) {
        return intValue(req.queryParams(name));
    }

    protected Long longArg(Request req, String name) {
        return longValue(req.queryParams(name));
    }

    protected String stringArg(Request req, String name) {
        return req.queryParams(name);
    }

    protected int[] intArrayArg(Request req, String name) {
        return intArrayValue(req.queryParams(name));
    }

    protected String[] stringArrayArg(Request req, String name) {
        return stringArrayValue(req.queryParams(name));
    }

    protected Integer intValue(String str) {
        return str == null ? null : Integer.parseInt(str);
    }

    protected Long longValue(String str) {
        return str == null ? null : Long.parseLong(str);
    }

    protected int[] intArrayValue(String str) {
        if (str == null) {
            return null;
        }
        if ("".equals(str)) {
            return new int[0];
        }
        return Arrays.stream(str.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    protected String[] stringArrayValue(String str) {
        if (str == null) {
            return null;
        }
        if ("".equals(str)) {
            return new String[0];
        }
        return str.split(",");
    }

    protected ThrowingFunction<String, ResultSet, Exception> resultSetProxyFactory = new ThrowingFunction<>() {
        private final Pattern connectionPattern = Pattern.compile("/connection/(\\d+)/");

        @Override
        public ResultSet apply(String rsUrl) throws Exception {
            String path = new URL(rsUrl).getPath();
            Matcher m = connectionPattern.matcher(path);
            if (!m.find()) {
                throw new IllegalStateException("Cannot find connection path in url " + rsUrl);
            }
            String connectionId = format("connection@%s", m.group(1));
            String jdbcUrl = ((Connection) attributes.computeIfAbsent(connectionId, s -> {
                throw new IllegalStateException(format("Connection %s not found", connectionId));
            })).getMetaData().getURL();

            String db = jdbcUrl.split(":")[1];

            ConnectionProperties connectionProperties = connectionPropertiesCache.computeIfAbsent(db, new Function<>() {
                @Override
                public ConnectionProperties apply(String db) {
                    InputStream in = getClass().getResourceAsStream(format("/conf/%s.properties", db));
                    if (in == null) {
                        return defaultConnectionProperties;
                    }
                    Properties props = new Properties();
                    try {
                        props.load(in);
                    } catch (IOException e) {
                        throw new IllegalStateException(e);
                    }
                    return new ConnectionProperties(props);
                }
            });

            return new ResultSetProxy(rsUrl, connectionProperties);
        }
    };
}

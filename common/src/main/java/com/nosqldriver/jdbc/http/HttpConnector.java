package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.TransportableException;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.nosqldriver.jdbc.http.Util.toByteArray;
import static java.lang.String.format;

public class HttpConnector {
    private final ObjectMapper objectMapper = new ObjectMapper();
    public <T> T get(String url, Class<T> clazz, String token) {
        try {
            HttpURLConnection httpConnection = (HttpURLConnection) new URL(url).openConnection();
            if (token != null) {
                httpConnection.setRequestProperty("HttpJdbcToken", token);
            }
            return retrieve(httpConnection, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> T delete(String url, Object payload, Class<T> clazz, String token) {
        return send(url, "DELETE", payload, clazz, token);
    }

    public <T> T post(String url, Object payload, Class<T> clazz, String token) {
        return send(url, "POST", payload, clazz, token);
    }

    public <T> T put(String url, Object payload, Class<T> clazz, String token) {
        return send(url, "PUT", payload, clazz, token);
    }

    private <T> T send(String url, String method, Object payload, Class<T> clazz, String token) {
        try {
            HttpURLConnection httpConnection = (HttpURLConnection) new URL(url).openConnection();
            httpConnection.setRequestMethod(method);
            httpConnection.setRequestProperty("Accept-Charset", StandardCharsets.UTF_8.name());
            httpConnection.setRequestProperty("Content-Type", "application/json");
            if (token != null) {
                httpConnection.setRequestProperty("HttpJdbcToken", token);
            }
            httpConnection.setDoOutput(true);
            httpConnection.setDoInput(true);
            objectMapper.writeValue(httpConnection.getOutputStream(), payload);
            return retrieve(httpConnection, clazz);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T retrieve(HttpURLConnection httpConnection, Class<T> clazz) throws IOException {
        InputStream in = httpConnection.getInputStream();
        int rc = httpConnection.getResponseCode();
        if (rc == 222) {
            SneakyThrower.sneakyThrow(objectMapper.readValue(in, TransportableException.class).getPayload());
        }
        byte[] content = toByteArray(in);
        return objectMapper.readValue(content, clazz);
    }

    public String buildUrl(String prefix, String[] ... params) {
        String queryString = Arrays.stream(params).filter(p -> p[1] != null).map(p -> format("%s=%s", p[0], Util.encode(p[1]))).collect(Collectors.joining("&"));
        return queryString.isEmpty() ? prefix : format("%s?%s", prefix, queryString) ;
    }

    public String buildUrl(String prefix, String suffix) {
        if (suffix == null || "".equals(suffix)) {
            return prefix;
        }
        prefix = prefix.endsWith("/") ? prefix.substring(0, prefix.length() - 1) : prefix;
        suffix = suffix.startsWith("/") ? suffix.substring(1) : suffix;
        return prefix + "/" + suffix;
    }
}

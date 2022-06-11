package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

public abstract class EntityProxy {
    @JsonProperty
    protected final String entityUrl;
    @JsonProperty
    protected final Class<?> clazz;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    protected EntityProxy(String entityUrl, Class<?> clazz) {
        this.entityUrl = entityUrl;
        this.clazz = clazz;
    }

    // setters are translated to HTTP PUT request although by the book PATCH should be used.
    // PUT is used instead of PATCH because client side uses good old standard java HTTP API that does not support PATCH
    // see HttpUrlConnection:
    // private static final String[] methods = {"GET", "POST", "HEAD", "OPTIONS", "PUT", "DELETE", "TRACE"};
    protected <T> void set(int parameterIndex, Class<?> type, String typeName, T value) {
        connector.put(entityUrl, new ParameterValue<>(parameterIndex, type, typeName, value, null), Void.class);
    }

    protected <T, A> void set(int parameterIndex, Class<?> type, String typeName, T value, A additionalArgument) {
        connector.put(entityUrl, new ParameterValue<>(parameterIndex, type, typeName, value, additionalArgument), Void.class);
    }

    protected <T, A> void set(int parameterIndex, Class<?> type, T value, A additionalArgument) {
        connector.put(entityUrl, new ParameterValue<>(parameterIndex, type, value, additionalArgument), Void.class);
    }

    protected <T> void set(int parameterIndex, Class<?> type, T value) {
        connector.put(entityUrl, new ParameterValue<>(parameterIndex, type, value), Void.class);
    }

    protected <T, A> void set(String parameterName, Class<?> type, String typeName, T value) {
        connector.put(entityUrl, new ParameterValue<>(parameterName, type, typeName, value, null), Void.class);
    }

    protected <T, A> void set(String parameterName, Class<?> type, String typeName, T value, A additionalArgument) {
        connector.put(entityUrl, new ParameterValue<>(parameterName, type, typeName, value, additionalArgument), Void.class);
    }

    protected <T, A> void set(String parameterName, Class<?> type, T value, A additionalArgument) {
        connector.put(entityUrl, new ParameterValue<>(parameterName, type, value, additionalArgument), Void.class);
    }

    protected <T> void set(String parameterName, Class<?> type, T value) {
        connector.put(entityUrl, new ParameterValue<>(parameterName, type, value), Void.class);
    }

    public String getEntityUrl() {
        return entityUrl;
    }
}

package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

import java.sql.SQLException;

abstract class EntityProxy {
    @JsonProperty
    protected final String entityUrl;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    protected EntityProxy(String entityUrl) {
        this.entityUrl = entityUrl;
    }

    protected <T, A> void set(int parameterIndex, Class<?> type, String typeName, T value) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterIndex, type, typeName, value, null), Void.class);
    }

    protected <T, A> void set(int parameterIndex, Class<?> type, String typeName, T value, A additionalArgument) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterIndex, type, typeName, value, additionalArgument), Void.class);
    }

    protected <T, A> void set(int parameterIndex, Class<?> type, T value, A additionalArgument) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterIndex, type, value, additionalArgument), Void.class);
    }

    protected <T> void set(int parameterIndex, Class<?> type, T value) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterIndex, type, value), Void.class);
    }

    protected <T, A> void set(String parameterName, Class<?> type, String typeName, T value) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterName, type, typeName, value, null), Void.class);
    }

    protected <T, A> void set(String parameterName, Class<?> type, String typeName, T value, A additionalArgument) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterName, type, typeName, value, additionalArgument), Void.class);
    }

    protected <T, A> void set(String parameterName, Class<?> type, T value, A additionalArgument) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterName, type, value, additionalArgument), Void.class);
    }

    protected <T> void set(String parameterName, Class<?> type, T value) throws SQLException {
        connector.patch(entityUrl, new ParameterValue<>(parameterName, type, value), Void.class);
    }
}

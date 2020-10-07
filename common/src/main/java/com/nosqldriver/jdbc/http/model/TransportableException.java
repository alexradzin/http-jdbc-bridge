package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.SQLException;

public class TransportableException {
    @JsonProperty
    private final String className;
    @JsonProperty
    private final String message;

    public TransportableException(Throwable e) {
        this(e.getClass().getName(), e.getMessage());
    }

    @JsonCreator
    public TransportableException(@JsonProperty("className") String className, @JsonProperty("message") String message) {
        this.className = className;
        this.message = message;
    }

    @JsonIgnore
    public Throwable getPayload() {
        try {
            return (Throwable)Class.forName(className).getConstructor(String.class).newInstance(message);
        } catch (ReflectiveOperationException e) {
            return new SQLException(message);
        }
    }
}

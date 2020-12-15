package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

public class TransportableException {
    @JsonProperty
    private final String[] classNames;
    @JsonProperty
    private final String message;

    public TransportableException(Throwable e) {
        this(getClassNames(e), e.getMessage());
    }

    @JsonCreator
    public TransportableException(@JsonProperty("className") String[] classNames, @JsonProperty("message") String message) {
        this.classNames = classNames;
        this.message = message;
    }

    @JsonIgnore
    public Throwable getPayload() {
        for (String className: classNames) {
            try {
                return (Throwable)Class.forName(className).getConstructor(String.class).newInstance(message);
            } catch (ReflectiveOperationException e) {
                // continue to the next class
            }
        }
        return new SQLException(message); // fallback to SQLException
    }

    private static String[] getClassNames(Throwable e) {
        Collection<String> classNames = new ArrayList<>();
        for (Class<?> c = e.getClass(); !Exception.class.equals(c); c = c.getSuperclass()) {
            classNames.add(c.getName());
        }
        return classNames.toArray(new String[0]);
    }
}

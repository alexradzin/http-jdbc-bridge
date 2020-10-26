package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ParameterValue<T, A> {
    private int index;
    private String name;
    private final Class<?> type;
    private final String typeName;
    private final T value;
    private final A additionalArgument;

    public ParameterValue(int index, Class<?> type, T value) {
        this(index, type, value, null);
    }
    public ParameterValue(int index, Class<?> type, T value, A length) {
        this(index, type, type == null ? null : type.getSimpleName(), value, length);
    }

    public ParameterValue(int parameterIndex, Class<?> type, String typeName, T value, A additionalArgument) {
        this.index = parameterIndex;
        this.type = type;
        this.typeName = typeName;
        this.value = value;
        this.additionalArgument = additionalArgument;
    }

    public ParameterValue(String name, Class<?> type, T value) {
        this(name, type, value, null);
    }

    public ParameterValue(String name, Class<?> type, T value, A additionalArgument) {
        this(name, type, type == null ? null : type.getSimpleName(), value, additionalArgument);
    }

    public ParameterValue(String name, Class<?> type, String typeName, T value, A additionalArgument) {
        this.name = name;
        this.type = type;
        this.typeName = typeName;
        this.value = value;
        this.additionalArgument = additionalArgument;
    }

    @JsonCreator
    public ParameterValue(
            @JsonProperty("type") Class<?> type,
            @JsonProperty("typeName") String typeName,
            @JsonProperty("value") T value,
            @JsonProperty("additionalArgument") A additionalArgument) {
        this.type = type;
        this.typeName = typeName;
        this.value = value;
        this.additionalArgument = additionalArgument;
    }


    public int getIndex() {
        return index;
    }

    public String getName() {
        return name;
    }

    public T getValue() {
        return value;
    }

    public A getAdditionalArgument() {
        return additionalArgument;
    }

    public Class<?> getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }
}

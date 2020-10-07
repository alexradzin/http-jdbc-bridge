package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OneParameterMetaData {
    private final int type;
    private final String typeName;
    private final String className;
    private final int nullable;
    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int mode;

    @JsonCreator
    public OneParameterMetaData(@JsonProperty("type") int type,
                                @JsonProperty("typeName") String typeName,
                                @JsonProperty("className") String className,
                                @JsonProperty("nullable") int nullable,
                                @JsonProperty("signed") boolean signed,
                                @JsonProperty("precision") int precision,
                                @JsonProperty("scale") int scale,
                                @JsonProperty("mode") int mode) {
        this.type = type;
        this.typeName = typeName;
        this.className = className;
        this.nullable = nullable;
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.mode = mode;
    }

    public int getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getClassName() {
        return className;
    }

    public int isNullable() {
        return nullable;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getMode() {
        return mode;
    }
}

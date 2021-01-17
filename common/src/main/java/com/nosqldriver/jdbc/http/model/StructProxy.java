package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

import static java.lang.String.format;

public class StructProxy extends EntityProxy implements Struct {
    @JsonProperty private String sqlTypeName;
    @JsonProperty private Object[] attributes;

    @JsonCreator
    public StructProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl, Struct.class);
    }

    public StructProxy(String entityUrl, Struct struct) throws SQLException {
        this(entityUrl, struct.getSQLTypeName(), struct.getAttributes());
    }

    public StructProxy(String entityUrl, String sqlTypeName, Object[] attributes) {
        super(entityUrl, Struct.class);
        this.sqlTypeName = sqlTypeName;
        this.attributes = attributes;
    }

    @Override
    @JsonProperty(value = "sqlTypeName")
    public String getSQLTypeName() throws SQLException {
        return sqlTypeName;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes;
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/attributes", entityUrl), map, Object[].class);
    }
}

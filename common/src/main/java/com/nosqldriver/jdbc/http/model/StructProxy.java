package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

public class StructProxy extends EntityProxy implements Struct {
    @JsonCreator
    public StructProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    public String getSQLTypeName() throws SQLException {
        return null;
    }

    @Override
    public Object[] getAttributes() throws SQLException {
        return new Object[0];
    }

    @Override
    public Object[] getAttributes(Map<String, Class<?>> map) throws SQLException {
        return new Object[0];
    }
}

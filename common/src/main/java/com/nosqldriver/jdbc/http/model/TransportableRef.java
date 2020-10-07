package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

public class TransportableRef extends EntityProxy implements Ref {
    private String baseTypeName;
    private Object object;

    @JsonCreator
    protected TransportableRef(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("baseTypeName") String baseTypeName) {
        super(entityUrl);
        this.baseTypeName = baseTypeName;
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName;
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        return connector.get(format("%s", entityUrl), Map.class);
    }

    @Override
    public Object getObject() throws SQLException {
        return object;
    }

    @Override
    public void setObject(Object value) throws SQLException {
        object = value;
    }
}

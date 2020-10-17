package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Ref;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

public class TransportableRef extends EntityProxy implements Ref {
    @JsonProperty private String baseTypeName;
    @JsonProperty private Object object;

    @JsonCreator
    protected TransportableRef(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    public TransportableRef(String entityUrl, Ref ref) throws SQLException {
        super(entityUrl);
        this.baseTypeName = ref.getBaseTypeName();
        this.object = ref.getObject();
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName;
    }

    @Override
    public Object getObject(Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/object", entityUrl), map, Object[].class);
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

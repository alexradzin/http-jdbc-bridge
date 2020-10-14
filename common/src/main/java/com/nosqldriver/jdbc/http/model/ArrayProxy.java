package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

public class ArrayProxy extends EntityProxy implements Array {
    private String baseTypeName;
    private int baseType;

    @JsonCreator
    public ArrayProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("token") String token) {
        super(entityUrl, token);
    }

    public ArrayProxy(String entityUrl, String token, Array array) throws SQLException {
        super(entityUrl, token);
        this.baseTypeName = array.getBaseTypeName();
        this.baseType = array.getBaseType();
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return baseType;
    }

    @Override
    @JsonIgnore
    public Object getArray() throws SQLException {
        return connector.get(format("%s/array", entityUrl), Object.class, token);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/array", entityUrl), map, Object.class, token);
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return connector.get(format("%s/array/%d/%d", entityUrl, index, count), Object.class, token);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/array/%d/%d", entityUrl, index, count), map, Object.class, token);
    }

    @Override
    @JsonIgnore
    public ResultSet getResultSet() throws SQLException {
        return connector.get(format("%s/resultset", entityUrl), ResultSetProxy.class, token);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/resultset", entityUrl), map, ResultSetProxy.class, token);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return connector.get(format("%s/resultset/%d/%d", entityUrl, index, count), ResultSetProxy.class, token);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/resultset/%d/%d", entityUrl, index, count), map, ResultSetProxy.class, token);
    }

    @Override
    public void free() throws SQLException {
        connector.delete(format("%s", entityUrl), null, Void.class, token);
    }
}

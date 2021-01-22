package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import static java.lang.String.format;

public class ArrayProxy extends EntityProxy implements Array, Serializable {
    private String baseTypeName = null;
    private int baseType = 0;

    @JsonCreator
    public ArrayProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl, Array.class);
    }

    public ArrayProxy(String entityUrl, Array array) throws SQLException {
        super(entityUrl, Array.class);
        this.baseTypeName = array.getBaseTypeName();
        this.baseType = array.getBaseType();
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        return baseTypeName == null ? connector.get(format("%s/basetypename", entityUrl), String.class) : baseTypeName;
    }

    @Override
    public int getBaseType() throws SQLException {
        return baseTypeName == null ? connector.get(format("%s/basetype", entityUrl), Integer.class) : baseType;
    }

    @Override
    @JsonIgnore
    public Object getArray() throws SQLException {
        return connector.get(format("%s/array", entityUrl), Object[].class);
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/array", entityUrl), map, Object[].class);
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        return connector.get(format("%s/array/%d/%d", entityUrl, index, count), Object[].class);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/array/%d/%d", entityUrl, index, count), map, Object[].class);
    }

    @Override
    @JsonIgnore
    public ResultSet getResultSet() throws SQLException {
        return connector.get(format("%s/resultset", entityUrl), ResultSetProxy.class);
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/resultset", entityUrl), map, ResultSetProxy.class);
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        return connector.get(format("%s/resultset?index=%d&count=%d", entityUrl, index, count), ResultSetProxy.class);
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return connector.post(format("%s/resultset?index=%d&count=%d", entityUrl, index, count), map, ResultSetProxy.class);
    }

    @Override
    public void free() throws SQLException {
        connector.delete(format("%s", entityUrl), null, Void.class);
    }
}

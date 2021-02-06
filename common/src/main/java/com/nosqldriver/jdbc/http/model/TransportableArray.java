package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Map;

public class TransportableArray implements Array {
    private final String baseTypeName;
    private final int baseType;
    private Object[] array;
    private final boolean partial;

    @JsonCreator
    public TransportableArray(String baseTypeName, int baseType, Object[] array) {
        this(baseTypeName, baseType, array, false);
    }

    public TransportableArray(String baseTypeName, int baseType, Object[] array, boolean partial) {
        this.baseTypeName = baseTypeName;
        this.baseType = baseType;
        this.array = array;
        this.partial = partial;
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
    public Object getArray() throws SQLException {
        throwIfPartial();
        return array;
    }

    @Override
    public Object getArray(Map<String, Class<?>> map) throws SQLException {
        return getArray();
    }

    @Override
    public Object getArray(long index, int count) throws SQLException {
        throwIfPartial();
        return Arrays.copyOfRange(array, (int)index - 1, (int)index - 1 + count);
    }

    @Override
    public Object getArray(long index, int count, Map<String, Class<?>> map) throws SQLException {
        throwIfPartial();
        return getArray(index, count);
    }

    @Override
    @JsonIgnore
    public ResultSet getResultSet() throws SQLException {
        throwIfPartial();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(Map<String, Class<?>> map) throws SQLException {
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet(long index, int count) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) throws SQLException {
        return getResultSet(index, count);
    }

    @Override
    public void free() throws SQLException {
        throwIfPartial();
        array = null;
    }

    private void throwIfPartial() throws SQLException {
        if (partial) {
            throw new SQLException("Cannot retrieve value from array");
        }
    }
}

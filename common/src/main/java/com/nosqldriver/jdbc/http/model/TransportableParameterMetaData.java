package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;

public class TransportableParameterMetaData extends WrapperProxy implements ParameterMetaData {
    private final List<OneParameterMetaData> parameters;

    @JsonCreator
    public TransportableParameterMetaData(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("parameters") List<OneParameterMetaData> parameters) {
        super(entityUrl);
        this.parameters = parameters;
    }

    @Override
    public int getParameterCount() throws SQLException {
        return parameters.size();
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return getParameter(param).isNullable();
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return getParameter(param).isSigned();
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return getParameter(param).getPrecision();
    }

    @Override
    public int getScale(int param) throws SQLException {
        return getParameter(param).getScale();
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return getParameter(param).getType();
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return getParameter(param).getTypeName();
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return getParameter(param).getClassName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return getParameter(param).getMode();
    }

    private OneParameterMetaData getParameter(int index) throws SQLException {
        if (index < 1 || index > parameters.size()) {
            throw new SQLException(format("Invalid column number %d", index));
        }
        return parameters.get(index - 1);
    }
}

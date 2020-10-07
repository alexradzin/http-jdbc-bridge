package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

import static java.lang.String.format;

public class ParameterMetaDataProxy extends WrapperProxy implements ParameterMetaData {
    @JsonCreator
    public ParameterMetaDataProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    @JsonIgnore
    public int getParameterCount() throws SQLException {
        return connector.get(format("%s/parameter/count", entityUrl), Integer.class);
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return connector.get(format("%s/nullable", entityUrl), Integer.class);
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return connector.get(format("%s/signed", entityUrl), Boolean.class);
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return connector.get(format("%s/precision", entityUrl), Integer.class);
    }

    @Override
    public int getScale(int param) throws SQLException {
        return connector.get(format("%s/scale", entityUrl), Integer.class);
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return connector.get(format("%s/parameter/type", entityUrl), Integer.class);
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return connector.get(format("%s/parameter/typename", entityUrl), String.class);
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return connector.get(format("%s/parameter/classname", entityUrl), String.class);
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return connector.get(format("%s/parameter/mode", entityUrl), Integer.class);
    }
}

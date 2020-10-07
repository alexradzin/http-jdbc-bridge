package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import static java.lang.String.format;

public class ResultSetMetaDataProxy extends WrapperProxy implements ResultSetMetaData {
    @JsonCreator
    public ResultSetMetaDataProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    @JsonIgnore
    public int getColumnCount() throws SQLException {
        return connector.get(format("%s/column/count", entityUrl), Integer.class);
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return connector.get(format("%s/autoincrement/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return connector.get(format("%s/casesensitive/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return connector.get(format("%s/searchable/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return connector.get(format("%s/currency/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return connector.get(format("%s/nullable/%d", entityUrl, column), Integer.class);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return connector.get(format("%s/signed/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return connector.get(format("%s/column/displaysize/%d", entityUrl, column), Integer.class);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return connector.get(format("%s/column/label/%d", entityUrl, column), String.class);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return connector.get(format("%s/column/name/%d", entityUrl, column), String.class);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return connector.get(format("%s/schema/name/%d", entityUrl, column), String.class);
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return connector.get(format("%s/precision/%d", entityUrl, column), Integer.class);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return connector.get(format("%s/scale/%d", entityUrl, column), Integer.class);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return connector.get(format("%s/table/name/%d", entityUrl, column), String.class);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return connector.get(format("%s/catalog/name/%d", entityUrl, column), String.class);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return connector.get(format("%s/column/type/%d", entityUrl, column), Integer.class);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return connector.get(format("%s/column/typename/%d", entityUrl, column), String.class);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return connector.get(format("%s/readonly/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return connector.get(format("%s/writable/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return connector.get(format("%s/definitelywritable/%d", entityUrl, column), Boolean.class);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return connector.get(format("%s/column/classname/%d", entityUrl, column), String.class);
    }
}

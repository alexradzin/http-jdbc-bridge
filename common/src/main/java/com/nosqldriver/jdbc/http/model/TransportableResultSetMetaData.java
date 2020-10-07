package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import static java.lang.String.format;

public class TransportableResultSetMetaData extends WrapperProxy implements ResultSetMetaData {
    private final List<ColumnMetaData> columns;

    @JsonCreator
    public TransportableResultSetMetaData(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("columns") List<ColumnMetaData> columns) {
        super(entityUrl);
        this.columns = columns;
    }

    @Override
    @JsonIgnore
    public int getColumnCount() throws SQLException {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return getColumn(column).isAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return getColumn(column).isCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return getColumn(column).isSearchable();
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return getColumn(column).isCurrency();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return getColumn(column).isNullable();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return getColumn(column).isSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return getColumn(column).getDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumn(column).getLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return getColumn(column).getName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return getColumn(column).getSchema();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return getColumn(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return getColumn(column).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return getColumn(column).getTable();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return getColumn(column).getCatalog();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return getColumn(column).getType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return getColumn(column).getTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return getColumn(column).isReadOnly();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return getColumn(column).isWritable();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return getColumn(column).isDefinitelyWritable();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return getColumn(column).getClassName();
    }

    protected ColumnMetaData getColumn(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columns.size()) {
            throw new SQLException(format("Invalid column number %d", columnIndex));
        }

        return columns.get(columnIndex - 1);
    }
}

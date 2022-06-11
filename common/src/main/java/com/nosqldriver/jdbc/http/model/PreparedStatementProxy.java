package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import static java.lang.String.format;

public class PreparedStatementProxy extends StatementProxy implements PreparedStatement {
    @JsonCreator
    public PreparedStatementProxy(@JsonProperty("entityUrl") String entityUrl) {
        this(entityUrl, PreparedStatement.class);
    }

    protected PreparedStatementProxy(String entityUrl, Class<?> clazz) {
        super(entityUrl, clazz);
    }

    @Override
    public ResultSet executeQuery() {
        return connector.get(format("%s/query", entityUrl), ResultSetProxy.class).withStatement(this);
    }

    @Override
    public int executeUpdate() {
        return connector.get(format("%s/update", entityUrl), Integer.class);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) {
        set(parameterIndex, null, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) {
        set(parameterIndex, boolean.class, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) {
        set(parameterIndex, byte.class, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) {
        set(parameterIndex, short.class, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) {
        set(parameterIndex, int.class, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) {
        set(parameterIndex, long.class, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) {
        set(parameterIndex, float.class, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) {
        set(parameterIndex, double.class, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) {
        set(parameterIndex, BigDecimal.class, x);
    }

    @Override
    public void setString(int parameterIndex, String x) {
        set(parameterIndex, String.class, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) {
        set(parameterIndex, byte[].class, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) {
        set(parameterIndex, Date.class, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) {
        set(parameterIndex, Time.class, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) {
        set(parameterIndex, Timestamp.class, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) {
        set(parameterIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) {
        set(parameterIndex, InputStream.class, "UnicodeStream", x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) {
        set(parameterIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void clearParameters() {
        connector.delete(format("%s", entityUrl), null, Void.class);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) {
        set(parameterIndex, Object.class, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) {
        set(parameterIndex, Object.class, x);
    }

    @Override
    public boolean execute() {
        return connector.get(format("%s/execute", entityUrl), Boolean.class);
    }

    @Override
    public void addBatch() {
        connector.put(format("%s/batch", entityUrl), null, Void.class);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) {
        set(parameterIndex, Reader.class, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) {
        set(parameterIndex, Ref.class, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) {
        set(parameterIndex, Blob.class, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) {
        set(parameterIndex, Clob.class, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) {
        set(parameterIndex, Array.class, x);
    }

    @Override
    @JsonIgnore
    public ResultSetMetaData getMetaData() {
        return connector.get(format("%s/metadata", entityUrl), TransportableResultSetMetaData.class);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) {
        set(parameterIndex, Date.class, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) {
        set(parameterIndex, Time.class, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) {
        set(parameterIndex, Timestamp.class, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) {
        set(parameterIndex, null, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) {
        set(parameterIndex, URL.class, x);
    }

    @Override
    @JsonIgnore
    public ParameterMetaData getParameterMetaData() {
        return connector.get(format("%s/parametermetadata", entityUrl), TransportableParameterMetaData.class);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) {
        set(parameterIndex, RowId.class, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) {
        set(parameterIndex, String.class, "NString", value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) {
        set(parameterIndex, Reader.class, "NCharacterStream", value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) {
        set(parameterIndex, NClob.class, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) {
        set(parameterIndex, Clob.class, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) {
        set(parameterIndex, Blob.class, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) {
        set(parameterIndex, NClob.class, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) {
        set(parameterIndex, SQLXML.class, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) {
        set(parameterIndex, Object.class, x, new int[] {targetSqlType, scaleOrLength}); //TODO: 2 arguments; is it OK?
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) {
        set(parameterIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) {
        set(parameterIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) {
        set(parameterIndex, Reader.class, "CharacterStream", reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) {
        set(parameterIndex, InputStream.class, "AsciiStream", x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) {
        set(parameterIndex, InputStream.class, "BinaryStream", x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) {
        set(parameterIndex, Reader.class, "CharacterStream", reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) {
        set(parameterIndex, Reader.class, "NCharacterStream", value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) {
        set(parameterIndex, Clob.class, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) {
        set(parameterIndex, Blob.class, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) {
        set(parameterIndex, NClob.class, reader);
    }

    @Override
    public PreparedStatementProxy withConnection(Connection connection) {
        super.withConnection(connection);
        return this;
    }

}

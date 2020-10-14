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
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import static java.lang.String.format;

public class PreparedStatementProxy extends StatementProxy implements PreparedStatement {
    @JsonCreator
    public PreparedStatementProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("token") String token) {
        super(entityUrl, token);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        return connector.get(format("%s/query", entityUrl), ResultSetProxy.class, token).withStatement(this);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return connector.get(format("%s/update", entityUrl), Integer.class, token);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        set(parameterIndex, (Class<?>)null, sqlType);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        set(parameterIndex, boolean.class, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        set(parameterIndex, byte.class, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        set(parameterIndex, short.class, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        set(parameterIndex, int.class, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        set(parameterIndex, long.class, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        set(parameterIndex, float.class, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        set(parameterIndex, double.class, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        set(parameterIndex, BigDecimal.class, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        set(parameterIndex, String.class, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        set(parameterIndex, byte[].class, x);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        set(parameterIndex, Date.class, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        set(parameterIndex, Time.class, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        set(parameterIndex, Timestamp.class, x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        set(parameterIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        set(parameterIndex, InputStream.class, "UnicodeStream", x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        set(parameterIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void clearParameters() throws SQLException {
        connector.delete(format("%s", entityUrl), null, Void.class, token);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        set(parameterIndex, Object.class, x, targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        set(parameterIndex, Object.class, x);
    }

    @Override
    public boolean execute() throws SQLException {
        return connector.get(format("%s/execute", entityUrl), Boolean.class, token);
    }

    @Override
    public void addBatch() throws SQLException {
        connector.put(format("%s/batch", entityUrl), null, Void.class, token);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        set(parameterIndex, Reader.class, reader, length);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        set(parameterIndex, Ref.class, x);
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        set(parameterIndex, Blob.class, x);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        set(parameterIndex, Clob.class, x);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        set(parameterIndex, Array.class, x);
    }

    @Override
    @JsonIgnore
    public ResultSetMetaData getMetaData() throws SQLException {
        return connector.get(format("%s/metadata", entityUrl), TransportableResultSetMetaData.class, token);
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        set(parameterIndex, Date.class, x, cal);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        set(parameterIndex, Time.class, x, cal);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        set(parameterIndex, Timestamp.class, x, cal);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        set(parameterIndex, (Class<?>)null, sqlType, typeName);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        set(parameterIndex, URL.class, x);
    }

    @Override
    @JsonIgnore
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return connector.get(format("%s/parametermetadata", entityUrl), TransportableParameterMetaData.class, token);
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        set(parameterIndex, RowId.class, x);
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        set(parameterIndex, String.class, "NString", value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        set(parameterIndex, Reader.class, "NCharacterStream", value, length);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        set(parameterIndex, NClob.class, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        set(parameterIndex, Clob.class, reader, length);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        set(parameterIndex, Blob.class, inputStream, length);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        set(parameterIndex, NClob.class, reader, length);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        set(parameterIndex, SQLXML.class, xmlObject);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        set(parameterIndex, Object.class, x, new int[] {targetSqlType, scaleOrLength}); //TODO: 2 arguments; is it OK?
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        set(parameterIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        set(parameterIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        set(parameterIndex, Reader.class, "CharacterStream", reader, length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        set(parameterIndex, InputStream.class, "AsciiStream", x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        set(parameterIndex, InputStream.class, "BinaryStream", x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        set(parameterIndex, Reader.class, "CharacterStream", reader);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        set(parameterIndex, Reader.class, "NCharacterStream", value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        set(parameterIndex, Clob.class, reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        set(parameterIndex, Blob.class, inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        set(parameterIndex, NClob.class, reader);
    }

    @Override
    public PreparedStatementProxy withConnection(Connection connection) {
        super.withConnection(connection);
        return this;
    }

}

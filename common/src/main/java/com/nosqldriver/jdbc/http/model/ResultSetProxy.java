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
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import static com.nosqldriver.jdbc.http.Util.encode;
import static java.lang.String.format;

public class ResultSetProxy extends WrapperProxy implements ResultSet {
    private Statement statement;
    private ResultSetMetaData md;

    @JsonCreator
    public ResultSetProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    public boolean next() throws SQLException {
        return connector.get(format("%s/next", entityUrl), Boolean.class);
    }

    @Override
    public void close() throws SQLException {
        connector.delete(format("%s", entityUrl), null, Void.class);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return connector.get(format("%s/wasnull", entityUrl), Boolean.class);
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        return connector.get(format("%s/string/index/%d", entityUrl, columnIndex), String.class);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return connector.get(format("%s/boolean/index/%d", entityUrl, columnIndex), boolean.class);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return connector.get(format("%s/byte/index/%d", entityUrl, columnIndex), byte.class);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return connector.get(format("%s/short/index/%d", entityUrl, columnIndex), short.class);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return connector.get(format("%s/int/index/%d", entityUrl, columnIndex), int.class);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return connector.get(format("%s/long/index/%d", entityUrl, columnIndex), long.class);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return connector.get(format("%s/float/index/%d", entityUrl, columnIndex), float.class);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return connector.get(format("%s/double/index/%d", entityUrl, columnIndex), Double.class);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return connector.get(format("%s/bigdecimal/index/%d", entityUrl, columnIndex), BigDecimal.class);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return connector.get(format("%s/bytes/index/%d", entityUrl, columnIndex), byte[].class);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return connector.get(format("%s/date/index/%d", entityUrl, columnIndex), Date.class);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return connector.get(format("%s/time/index/%d", entityUrl, columnIndex), Time.class);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return connector.get(format("%s/timestamp/index/%d", entityUrl, columnIndex), Timestamp.class);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return connector.get(format("%s/ascii/streem/index/%d", entityUrl, columnIndex), InputStream.class);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return connector.get(format("%s/unicode/streem/index/%d", entityUrl, columnIndex), InputStream.class);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return connector.get(format("%s/binary/streem/index/%d", entityUrl, columnIndex), InputStream.class);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return connector.get(format("%s/string/label/%s", entityUrl, columnLabel), String.class);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return connector.get(format("%s/boolean/label/%s", entityUrl, columnLabel), boolean.class);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return connector.get(format("%s/byte/label/%s", entityUrl, encode(columnLabel)), byte.class);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return connector.get(format("%s/short/label/%s", entityUrl, encode(columnLabel)), short.class);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return connector.get(format("%s/int/label/%s", entityUrl, encode(columnLabel)), int.class);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return connector.get(format("%s/long/label/%s", entityUrl, encode(columnLabel)), long.class);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return connector.get(format("%s/float/label/%s", entityUrl, encode(columnLabel)), float.class);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return connector.get(format("%s/double/label/%s", entityUrl, encode(columnLabel)), double.class);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return connector.get(format("%s/bigdecimal/label/%s", entityUrl, encode(columnLabel)), BigDecimal.class);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return connector.get(format("%s/bytes/label/%s", entityUrl, encode(columnLabel)), byte[].class);
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return connector.get(format("%s/date/label/%s", entityUrl, encode(columnLabel)), Date.class);
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return connector.get(format("%s/time/label/%s", entityUrl, encode(columnLabel)), Time.class);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return connector.get(format("%s/timestamp/label/%s", entityUrl, encode(columnLabel)), Timestamp.class);
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return connector.get(format("%s/asciistream/label/%s", entityUrl, encode(columnLabel)), InputStream.class);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return connector.get(format("%s/unicodestream/label/%s", entityUrl, columnLabel), InputStream.class);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return connector.get(format("%s/binarystream/label/%s", entityUrl, encode(columnLabel)), InputStream.class);
    }

    @Override
    @JsonIgnore
    public SQLWarning getWarnings() throws SQLException {
        return connector.get(format("%s/warnings", entityUrl), TransportableSQLWarning.class);
    }

    @Override
    public void clearWarnings() throws SQLException {
        connector.delete(format("%s/warnings", entityUrl), null, Void.class);
    }

    @Override
    @JsonIgnore
    public String getCursorName() throws SQLException {
        return connector.get(format("%s/cursorname", entityUrl), String.class);
    }

    @Override
    @JsonIgnore
    public ResultSetMetaData getMetaData() throws SQLException {
        if (md == null) {
            md = connector.get(format("%s/metadata", entityUrl), ResultSetMetaDataProxy.class);
        }
        return md;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return connector.get(format("%s/object/index/%s", entityUrl, columnIndex), Object.class);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return connector.get(format("%s/object/label/%s", entityUrl, columnLabel), Object.class);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return connector.get(format("%s/column/label/%s", entityUrl, encode(columnLabel)), Integer.class);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return connector.get(format("%s/characterstream/index/%d", entityUrl, columnIndex), Reader.class);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return connector.get(format("%s/characterstream/label/%s", entityUrl, encode(columnLabel)), Reader.class);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return connector.get(format("%s/bigdecimal/index/%s", entityUrl, columnIndex), BigDecimal.class);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return connector.get(format("%s/bigdecimal/label/%s", entityUrl, columnLabel), BigDecimal.class);
    }

    @Override
    @JsonIgnore
    public boolean isBeforeFirst() throws SQLException {
        return connector.get(format("%s/before/first", entityUrl), boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isAfterLast() throws SQLException {
        return connector.get(format("%s/after/last", entityUrl), boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isFirst() throws SQLException {
        return connector.get(format("%s/first", entityUrl), boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isLast() throws SQLException {
        return connector.get(format("%s/last", entityUrl), Boolean.class);
    }

    @Override
    public void beforeFirst() throws SQLException {
        connector.post(format("%s/before/first", entityUrl), null, Void.class);
    }

    @Override
    public void afterLast() throws SQLException {
        connector.post(format("%s/after/last", entityUrl), null, Void.class);
    }

    @Override
    public boolean first() throws SQLException {
        return connector.post(format("%s/first", entityUrl), null, Boolean.class);
    }

    @Override
    public boolean last() throws SQLException {
        return connector.post(format("%s/last", entityUrl), null, Boolean.class);
    }

    @Override
    @JsonIgnore
    public int getRow() throws SQLException {
        return connector.get(format("%s/row", entityUrl), int.class);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        return connector.get(format("%s/absolute/%d", entityUrl, row), boolean.class);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        return connector.get(format("%s/relative/%d", entityUrl, rows), boolean.class);
    }

    @Override
    public boolean previous() throws SQLException {
        return connector.get(format("%s/previous", entityUrl), boolean.class);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        connector.post(format("%s/fetch/direction", entityUrl), direction, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchDirection() throws SQLException {
        return connector.get(format("%s/fetch/direction", entityUrl), int.class);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        connector.post(format("%s/fetch/size", entityUrl), rows, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchSize() throws SQLException {
        return connector.get(format("%s/fetch/size", entityUrl), int.class);
    }

    @Override
    @JsonIgnore
    public int getType() throws SQLException {
        return connector.get(format("%s/type", entityUrl), int.class);
    }

    @Override
    @JsonIgnore
    public int getConcurrency() throws SQLException {
        return connector.get(format("%s/concurrency", entityUrl), int.class);
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return connector.get(format("%s/row/updated", entityUrl), boolean.class);
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return connector.get(format("%s/row/inserted", entityUrl), boolean.class);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return connector.get(format("%s/row/deleted", entityUrl), boolean.class);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        super.set(columnIndex, (Class<?>)null, null);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        set(columnIndex, boolean.class, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        set(columnIndex, Byte.class, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        set(columnIndex, Short.class, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        set(columnIndex, int.class, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        set(columnIndex, long.class, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        set(columnIndex, float.class, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        set(columnIndex, double.class, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        set(columnIndex, BigDecimal.class, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        set(columnIndex, String.class, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        set(columnIndex, byte[].class, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        set(columnIndex, Date.class, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        set(columnIndex, Time.class, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        set(columnIndex, Timestamp.class, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        set(columnIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        set(columnIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        set(columnIndex, Reader.class, "CharacterStream", x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        set(columnIndex, Object.class, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        set(columnIndex, Object.class, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        set(columnLabel, (Class<?>)null, null);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        set(columnLabel, boolean.class, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        set(columnLabel, byte.class, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        set(columnLabel, short.class, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        set(columnLabel, int.class, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        set(columnLabel, long.class, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        set(columnLabel, float.class, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        set(columnLabel, double.class, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        set(columnLabel, BigDecimal.class, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        set(columnLabel, String.class, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        set(columnLabel, byte[].class, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        set(columnLabel, Date.class, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        set(columnLabel, Time.class, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        set(columnLabel, Timestamp.class, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        set(columnLabel, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        set(columnLabel, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        set(columnLabel, Reader.class, "CharacterStream", reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        set(columnLabel, Object.class, x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        set(columnLabel, Object.class, x);
    }

    @Override
    public void insertRow() throws SQLException {
        connector.post(format("%s/row", entityUrl), null, Void.class);
    }

    @Override
    public void updateRow() throws SQLException {
        connector.put(format("%s/row", entityUrl), null, Void.class);
    }

    @Override
    public void deleteRow() throws SQLException {
        connector.delete(format("%s/row", entityUrl), null, Void.class);
    }

    @Override
    public void refreshRow() throws SQLException {
        connector.get(format("%s/row", entityUrl), Void.class);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        connector.put(format("%s/row", entityUrl), "cancel", Void.class);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        connector.post(format("%s/move", entityUrl), "insert", Void.class);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        connector.post(format("%s/move", entityUrl), "current", Void.class);
    }

    @Override
    @JsonIgnore
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        return connector.get(format("%s/ref/index/%d", entityUrl, columnIndex), TransportableRef.class);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return connector.get(format("%s/blob/index/%d", entityUrl, columnIndex), BlobProxy.class);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return connector.get(format("%s/clob/index/%d", entityUrl, columnIndex), ClobProxy.class);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return connector.get(format("%s/array/index/%d", entityUrl, columnIndex), ArrayProxy.class);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        //TODO implement!
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return connector.get(format("%s/ref/label/%s", entityUrl, columnLabel), TransportableRef.class);
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return connector.get(format("%s/blob/label/%s", entityUrl, columnLabel), BlobProxy.class);
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return connector.get(format("%s/clob/label/%s", entityUrl, columnLabel), ClobProxy.class);
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return connector.get(format("%s/array/label/%s", entityUrl, columnLabel), ArrayProxy.class);
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return connector.get(format("%s/date/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Date.class);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return connector.get(format("%s/date/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Date.class);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return connector.get(format("%s/time/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Time.class);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return connector.get(format("%s/time/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Time.class);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return connector.get(format("%s/timestamp/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Timestamp.class);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return connector.get(format("%s/timestamp/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Timestamp.class);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return connector.get(format("%s/url/index/%d", entityUrl, columnIndex), URL.class);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return connector.get(format("%s/url/label/%s", entityUrl, columnLabel), URL.class);
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        set(columnIndex, Ref.class, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        set(columnLabel, Ref.class, x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        set(columnIndex, Blob.class, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        set(columnLabel, Ref.class, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        set(columnIndex, Clob.class, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        set(columnLabel, Clob.class, x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        set(columnIndex, Array.class, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        set(columnLabel, Array.class, x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        return connector.get(format("%s/rowid/label/%s", entityUrl, columnIndex), RowId.class);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return connector.get(format("%s/rowid/label/%s", entityUrl, columnLabel), RowId.class);
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        set(columnIndex, RowId.class, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        set(columnLabel, RowId.class, x);
    }

    @Override
    @JsonIgnore
    public int getHoldability() throws SQLException {
        return connector.get(format("%s/hodability", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public boolean isClosed() throws SQLException {
        return connector.get(format("%s/closed", entityUrl), Boolean.class);
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        set(columnIndex, String.class, "NString", nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        set(columnLabel, String.class, "NString", nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        set(columnIndex, NClob.class, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        set(columnLabel, NClob.class, nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        return connector.get(format("%s/nclob/index/%d", entityUrl, columnIndex), ClobProxy.class);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return connector.get(format("%s/nclob/label/%s", entityUrl, columnLabel), ClobProxy.class);
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return connector.get(format("%s/sqlxml/index/%s", entityUrl, columnIndex), SQLXML.class);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return connector.get(format("%s/sqlxml/label/%s", entityUrl, columnLabel), SQLXML.class);
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        set(columnIndex, SQLXML.class, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        set(columnLabel, SQLXML.class, xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return connector.get(format("%s/nstring/index/%d", entityUrl, columnIndex), String.class);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return connector.get(format("%s/nstring/label/%s", entityUrl, columnLabel), String.class);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return connector.get(format("%s/ncharacterstream/index/%d", entityUrl, columnIndex), Reader.class);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return connector.get(format("%s/ncharacterstream/label/%s", entityUrl, columnLabel), Reader.class);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        set(columnIndex, Reader.class, "NCharacterStream", x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        set(columnLabel, Reader.class, "NCharacterStream", reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        set(columnIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        set(columnIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        set(columnIndex, Reader.class, "CharacterStream", x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        set(columnLabel, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        set(columnLabel, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        set(columnLabel, InputStream.class, "CharacterStream", reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        set(columnIndex, Blob.class, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        set(columnLabel, Blob.class, inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        set(columnIndex, Clob.class, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        set(columnLabel, Clob.class, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        set(columnIndex, NClob.class, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        set(columnLabel, NClob.class, reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        set(columnIndex, Reader.class, "NCharacterStream", x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        set(columnLabel, Reader.class, "NCharacterStream", reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        set(columnIndex, InputStream.class, "AsciiStream", x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        set(columnIndex, InputStream.class, "BinaryStream", x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        set(columnIndex, Reader.class, "CharacterStream", x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        set(columnLabel, InputStream.class, "AsciiStream", x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        set(columnLabel, InputStream.class, "BinaryStream", x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        set(columnLabel, Reader.class, "CharacterStream", reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        set(columnIndex, Blob.class, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        set(columnLabel, Blob.class, inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        set(columnIndex, Clob.class, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        set(columnLabel, Clob.class, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        set(columnIndex, NClob.class, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        set(columnLabel, NClob.class, reader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return connector.get(format("%s/object/index/%d/%s", entityUrl, columnIndex, type), type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return connector.get(format("%s/object/label/%s/%s", entityUrl, columnLabel, type), type);
    }

    public ResultSetProxy withStatement(Statement statement) {
        this.statement = statement;
        return this;
    }

    private String calendarParameter(Calendar cal) {
        // TODO: try to add some support of Locale: it can be passed to constructor but is not stored as-is in Calendar: parts are used instead
        return format("tz=%s,millis=%d", cal.getTimeZone().getID(), cal.getTimeInMillis());
    }
}

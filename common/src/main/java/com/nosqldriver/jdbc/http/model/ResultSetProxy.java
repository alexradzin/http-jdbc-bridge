package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

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
import java.util.AbstractMap.SimpleEntry;
import java.util.Calendar;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.jdbc.http.Util.encode;
import static java.lang.String.format;

public class ResultSetProxy extends WrapperProxy implements ResultSet {
    private static final Map<Class<?>, Function<Object, Object>> bigDecimalCasters = Stream.of(
            new SimpleEntry<Class<?>, Function<Object, Object>>(Byte.class, e -> BigDecimal.valueOf((Byte)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(byte.class, e -> BigDecimal.valueOf((byte)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Short.class, e -> BigDecimal.valueOf((Short)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(short.class, e -> BigDecimal.valueOf((short)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Integer.class, e -> BigDecimal.valueOf((Integer)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(int.class, e -> BigDecimal.valueOf((int)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Long.class, e -> BigDecimal.valueOf((Long)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(long.class, e -> BigDecimal.valueOf((long)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Float.class, e -> BigDecimal.valueOf((Float)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(float.class, e -> BigDecimal.valueOf((float)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Double.class, e -> BigDecimal.valueOf((Double)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(double.class, e -> BigDecimal.valueOf((double)e))
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    private static final Map<Class<?>, Function<Object, Object>> casters = Stream.of(
            new SimpleEntry<Class<?>, Function<Object, Object>>(Byte.class, e -> ((Number)e).byteValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(byte.class, e -> ((Number)e).byteValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Short.class, e -> ((Number)e).shortValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(short.class, e -> ((Number)e).shortValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Integer.class, e -> ((Number)e).intValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(int.class, e -> ((Number)e).intValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Long.class, e -> ((Number)e).longValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(long.class, e -> ((Number)e).longValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(BigDecimal.class, e -> bigDecimalCasters.get(e.getClass()).apply(e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Float.class, e -> ((Number)e).floatValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(float.class, e -> ((Number)e).floatValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Double.class, e -> ((Number)e).doubleValue()),
            new SimpleEntry<Class<?>, Function<Object, Object>>(double.class, e -> ((Number)e).doubleValue())
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));




    private Statement statement;
    private ResultSetMetaData md;
    private volatile RowData rowData = null;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @JsonCreator
    public ResultSetProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    public boolean next() throws SQLException {
        rowData = connector.get(format("%s/nextrow", entityUrl), RowData.class);
        return rowData.isMoved();
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
        return getValue("index", columnIndex, String.class, columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, boolean.class, columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, byte.class, columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, short.class, columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, int.class, columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, long.class, columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, float.class, columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, double.class, columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        return getValue("index", columnIndex, BigDecimal.class, columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, byte[].class, "bytes", columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Date.class, columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Time.class, columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Timestamp.class, columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, InputStream.class, "ascii/stream", columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, InputStream.class, "unicode/stream", columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, InputStream.class, "binary/stream", columnIndex);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, String.class, getIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, boolean.class, getIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, byte.class, getIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, short.class, getIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, int.class, getIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, long.class, getIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, float.class, getIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, double.class, getIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getValue("label", columnLabel, BigDecimal.class, getIndex(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, byte[].class, "bytes", getIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Date.class, getIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Time.class, getIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Timestamp.class, getIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, InputStream.class, "ascii/stream", getIndex(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, InputStream.class, "unicode/stream", getIndex(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, InputStream.class, "binary/stream", getIndex(columnLabel));
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
            md = connector.get(format("%s/metadata", entityUrl), TransportableResultSetMetaData.class);
        }
        return md;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Object.class, columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Object.class, getIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        return connector.get(format("%s/column/label/%s", entityUrl, encode(columnLabel)), Integer.class);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Reader.class, "character/stream", columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Reader.class, "character/stream", getIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, BigDecimal.class, columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, BigDecimal.class, getIndex(columnLabel));
    }

    @Override
    @JsonIgnore
    public boolean isBeforeFirst() throws SQLException {
        return connector.get(format("%s/before/firstrow", entityUrl), boolean.class);
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
        rowData = null;
        connector.post(format("%s/before/first", entityUrl), null, Void.class);
    }

    @Override
    public void afterLast() throws SQLException {
        rowData = null;
        connector.post(format("%s/after/last", entityUrl), null, Void.class);
    }

    @Override
    public boolean first() throws SQLException {
        rowData = connector.get(format("%s/firstrow", entityUrl), RowData.class);
        return rowData.isMoved();
    }

    @Override
    public boolean last() throws SQLException {
        rowData = connector.get(format("%s/lastrow", entityUrl), RowData.class);
        return rowData.isMoved();
    }

    @Override
    @JsonIgnore
    public int getRow() throws SQLException {
        return connector.get(format("%s/row", entityUrl), int.class);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        rowData = connector.get(format("%s/absolute/%d", entityUrl, row), RowData.class);
        return rowData.isMoved();
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        rowData = connector.get(format("%s/relative/%d", entityUrl, rows), RowData.class);
        return rowData.isMoved();
    }

    @Override
    public boolean previous() throws SQLException {
        rowData = connector.get(format("%s/previousrow", entityUrl), RowData.class);
        return rowData.isMoved();
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
        return getValue("index", columnIndex, Ref.class, columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Blob.class, columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Clob.class, columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Array.class, columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        //TODO implement!
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, TransportableRef.class, "ref", getIndex(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Blob.class, getIndex(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Clob.class, getIndex(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Array.class, getIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return rowData == null ? connector.get(format("%s/date/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Date.class) : (Date)rowData.getRow()[columnIndex - 1];
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return rowData == null ? connector.get(format("%s/date/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Date.class) : (Date)rowData.getRow()[getIndex(columnLabel)];
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return rowData == null ? connector.get(format("%s/time/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Time.class) : (Time)rowData.getRow()[columnIndex - 1];
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return rowData == null ? connector.get(format("%s/time/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Time.class) : (Time)rowData.getRow()[getIndex(columnLabel)];
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return rowData == null ? connector.get(format("%s/timestamp/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Timestamp.class) : (Timestamp)rowData.getRow()[columnIndex - 1];
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return rowData == null ? connector.get(format("%s/timestamp/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Timestamp.class) : (Timestamp)rowData.getRow()[getIndex(columnLabel)];
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, URL.class, columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, URL.class, getIndex(columnLabel));
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
        return getValue("index", columnIndex, RowId.class, columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, RowId.class, getIndex(columnLabel));
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
        return connector.get(format("%s/holdability", entityUrl), Integer.class);
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
        return getValue("index", columnIndex, NClob.class, columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, NClob.class, getIndex(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, SQLXML.class, columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, SQLXML.class, getIndex(columnLabel));
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
        return getValue("index", columnIndex, String.class, "nstring", columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, String.class, "nstring", getIndex(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getValue("index", columnIndex, Reader.class, "ncharacter/stream", columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getValue("label", columnLabel, Reader.class, "ncharacter/stream", getIndex(columnLabel));
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
        return rowData == null ? connector.get(format("%s/object/index/%d/%s", entityUrl, columnIndex, type), type) : cast(rowData.getRow()[columnIndex - 1], type);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return rowData == null ? connector.get(format("%s/object/label/%s/%s", entityUrl, columnLabel, type), type) : cast(rowData.getRow()[getIndex(columnLabel)], type);
    }

    public ResultSetProxy withStatement(Statement statement) {
        this.statement = statement;
        return this;
    }

    private String calendarParameter(Calendar cal) {
        // TODO: try to add some support of Locale: it can be passed to constructor but is not stored as-is in Calendar: parts are used instead
        return format("tz=%s,millis=%d", cal.getTimeZone().getID(), cal.getTimeInMillis());
    }

    private <T, M> T getValue(String markerName, M columnMarker, Class<T> clazz, Integer columnIndex) {
        return getValue(markerName, columnMarker, clazz, clazz.getSimpleName().toLowerCase(), columnIndex);
    }

    private <T, M> T getValue(String markerName, M columnMarker, Class<T> clazz, String typeName, Integer columnIndex) {
        return rowData == null || columnIndex == null ? connector.get(format("%s/%s/%s/%s", entityUrl, typeName, markerName, columnMarker), clazz) : cast(rowData.getRow()[columnIndex - 1], clazz);
    }

    private Integer getIndex(String columnLabel) throws SQLException {
        return ((TransportableResultSetMetaData)getMetaData()).getIndex(columnLabel);
    }

    private <T> T cast(Object obj, Class<T> clazz) {
        if (obj == null) {
            return null;
        }
        Function<Object, Object> caster = casters.get(clazz);
        //noinspection unchecked
        try {
            if (caster != null) {
                return (T)caster.apply(obj);
            }
            if (obj instanceof String && String.class.equals(clazz)) {
                return (T)obj;
            }
            return objectMapper.readValue(obj instanceof String ? "\"" + obj + "\"" : "" + obj, clazz);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

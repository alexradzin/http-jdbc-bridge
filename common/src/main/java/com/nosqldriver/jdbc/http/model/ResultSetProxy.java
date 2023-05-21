package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.model.ConnectionProperties.StreamType;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingFunction;
import com.nosqldriver.util.function.ThrowingSupplier;

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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.AbstractMap.SimpleEntry;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.jdbc.http.Util.encode;
import static com.nosqldriver.jdbc.http.model.ConnectionProperties.StreamType.ASCII;
import static com.nosqldriver.jdbc.http.model.ConnectionProperties.StreamType.UNICODE;
import static java.lang.String.format;

public class ResultSetProxy extends WrapperProxy implements ResultSet {
    private static final Map<Class<?>, Function<Object, Object>> bigDecimalCasters = Stream.of(
            new SimpleEntry<Class<?>, Function<Object, Object>>(Boolean.class, e -> e == null ? false : BigDecimal.valueOf(((Boolean)e) ? 1 : 0)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Byte.class, e -> e == null ? 0 : BigDecimal.valueOf((Byte)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(byte.class, e -> e == null ? 0 : BigDecimal.valueOf((byte)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Short.class, e -> e == null ? 0 : BigDecimal.valueOf((Short)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(short.class, e -> e == null ? 0 : BigDecimal.valueOf((short)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Integer.class, e -> e == null ? 0 : BigDecimal.valueOf((Integer)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(int.class, e -> e == null ? 0 : BigDecimal.valueOf((int)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Long.class, e -> e == null ? 0 : BigDecimal.valueOf((Long)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(long.class, e -> e == null ? 0 : BigDecimal.valueOf((long)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Float.class, e -> e == null ? 0.0 : BigDecimal.valueOf((Float)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(float.class, e -> e == null ? 0.0 : BigDecimal.valueOf((float)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(Double.class, e -> e == null ? 0.0 : BigDecimal.valueOf((Double)e)),
            new SimpleEntry<Class<?>, Function<Object, Object>>(double.class, e -> e == null ? 0.0 : BigDecimal.valueOf((double)e))
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    private static final Function<Object, Object> bigDecimalUnacceptable = o -> {
        throw new IllegalArgumentException(format("Value '%s' is outside of valid range for type %s", o, BigDecimal.class));
    };

    private static class CastorArg {
        private final Object obj;
        private final Class<?> from;
        private final Class<?> to;
        private final int sqlType;
        private final int columnIndex;

        public CastorArg(Object obj, Class<?> from, Class<?> to, int sqlType, int columnIndex) {
            this.obj = obj;
            this.from = from;
            this.to = to;
            this.sqlType = sqlType;
            this.columnIndex = columnIndex;
        }

        public Object getObj() {
            return obj;
        }

        public Class<?> getFrom() {
            return from;
        }

        public Class<?> getTo() {
            return to;
        }

        public int getSqlType() {
            return sqlType;
        }

        public int getColumnIndex() {
            return columnIndex;
        }
    }

    private final Map<Class<?>, ThrowingFunction<CastorArg, ? extends Object, SQLException>> casters;

    private static class NumericCastor<T> implements ThrowingFunction<CastorArg, T, SQLException> {
        private final Predicate<Object> checker;
        private final Function<Object, T> actualCastor;
        private final ThrowingFunction<Boolean, T, SQLException> booleanCastor;
        private final Class<T> type;
        private final Function<Character, T> charToNumber;

        private NumericCastor(Predicate<Object> checker, Function<Object, T> actualCastor, ThrowingFunction<Boolean, T, SQLException> booleanCastor, Class<T> type) {
            this(checker, actualCastor, booleanCastor, type, null);
        }

        private NumericCastor(Predicate<Object> checker, Function<Object, T> actualCastor, ThrowingFunction<Boolean, T, SQLException> booleanCastor, Class<T> type, Function<Character, T> charToNumber) {
            this.checker = checker;
            this.actualCastor = actualCastor;
            this.booleanCastor = booleanCastor;
            this.type = type;
            this.charToNumber = charToNumber;
        }


        @Override
        public T apply(CastorArg arg) throws SQLException {
            Object o = arg.getObj();
            if (o == null) {
                return (T)defaultValues.get(arg.getTo());
            }
            if (o instanceof Boolean) {
                return booleanCastor.apply((Boolean)o);
            }
            if (arg.getSqlType() == Types.CHAR && charToNumber != null) {
                return charToNumber.apply(((String)o).charAt(0));
            }
            if (!checker.test(o)) {
                throw new IllegalArgumentException(format("Value '%s' is outside of valid range for type %s", o, type));
            }
            return actualCastor.apply(o);
        }
    }

    private static final Map<Class<?>, Object> defaultValues = Stream.of(
            new SimpleEntry<>(Byte.class, (byte)0),
            new SimpleEntry<>(Short.class, (short)0),
            new SimpleEntry<>(Integer.class, 0),
            new SimpleEntry<>(Long.class, 0L),
            new SimpleEntry<>(Float.class, 0.0f),
            new SimpleEntry<>(Double.class, 0.0),
            new SimpleEntry<>(Boolean.class, false),
            new SimpleEntry<>(byte.class, (byte)0),
            new SimpleEntry<>(short.class, (short)0),
            new SimpleEntry<>(int.class, 0),
            new SimpleEntry<>(long.class, 0L),
            new SimpleEntry<>(float.class, 0.0f),
            new SimpleEntry<>(double.class, 0.0),
            new SimpleEntry<>(boolean.class, false)
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    @JsonProperty
    private final ConnectionProperties connectionProperties;
    private Statement statement;
    private ResultSetMetaData md;
    // TODO: fix multi-threading support (rowData and wasNull)
    private RowData[] rows = null;
    private int localRowIndex = 0;
    private volatile boolean wasNull = false;
    private volatile boolean closed = false;

    // This constructor is temporary patch
    public ResultSetProxy(@JsonProperty("entityUrl") String entityUrl) {
        this(entityUrl, new ConnectionProperties(System.getProperties()));
    }

    @JsonCreator
    public ResultSetProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("connectionProperties") ConnectionProperties connectionProperties) {
        super(entityUrl, ResultSet.class);
        this.connectionProperties = connectionProperties;
        casters = new HashMap<>();

        casters.put(byte.class, new NumericCastor<>(e -> inRange(e, Byte.MIN_VALUE, Byte.MAX_VALUE), e -> (byte)connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (byte)(b ? 1 : 0)), Byte.class, connectionProperties.isCharToByte() ? connectionProperties::toByte: null));
        casters.put(short.class, new NumericCastor<>(e -> inRange(e, Short.MIN_VALUE, Short.MAX_VALUE), e -> (short)connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (short)(b ? 1 : 0)), Short.class));
        casters.put(int.class, new NumericCastor<>(e -> inRange(e, Integer.MIN_VALUE, Integer.MAX_VALUE), e -> (int)connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (b ? 1 : 0)), Integer.class));
        casters.put(long.class, new NumericCastor<>(e -> inRange(e, Long.MIN_VALUE, Long.MAX_VALUE), e -> connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (long)(b ? 1 : 0)), Long.class));
        casters.put(float.class, new NumericCastor<>(e -> inRange(e, -Float.MAX_VALUE, Float.MAX_VALUE), e -> ((Number) e).floatValue(), connectionProperties.booleanToNumber(f -> (f ? 1.f : 0.f)), float.class));
        casters.put(double.class, new NumericCastor<>(e -> inRange(e, -Double.MAX_VALUE, Double.MAX_VALUE), e -> ((Number) e).doubleValue(), connectionProperties.booleanToNumber(f -> (f ? 1. : 0.f)), double.class));
        casters.put(Byte.class, new NumericCastor<>(e -> inRange(e, Byte.MIN_VALUE, Byte.MAX_VALUE), e -> (byte)connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (byte)(b ? 1 : 0)), Byte.class, connectionProperties.isCharToByte() ? c -> (byte)connectionProperties.toByte(c) : null));
        casters.put(Short.class, new NumericCastor<>(e -> inRange(e, Short.MIN_VALUE, Short.MAX_VALUE), e -> (short)connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (short)(b ? 1 : 0)), Short.class));
        casters.put(Integer.class, new NumericCastor<>(e -> inRange(e, Integer.MIN_VALUE, Integer.MAX_VALUE), e -> (int)connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (b ? 1 : 0)), Integer.class));
        casters.put(Long.class, new NumericCastor<>(e -> inRange(e, Long.MIN_VALUE, Long.MAX_VALUE), e -> connectionProperties.toInteger(((Number) e).doubleValue()), connectionProperties.booleanToNumber(b -> (long)(b ? 1 : 0)), Long.class));
        casters.put(Float.class, new NumericCastor<>(e -> inRange(e, -Float.MAX_VALUE, Float.MAX_VALUE), e -> ((Number) e).floatValue(), connectionProperties.booleanToNumber(f -> (f ? 1.f : 0.f)), Float.class));
        casters.put(Double.class, new NumericCastor<>(e -> inRange(e, -Double.MAX_VALUE, Double.MAX_VALUE), e -> ((Number) e).doubleValue(), connectionProperties.booleanToNumber(f -> (f ? 1. : 0.f)), Double.class));

        casters.put(BigDecimal.class, arg -> {
            Object o = arg.getObj();
            if (o == null) {
                return null;
            }
            Object value = Optional.ofNullable(bigDecimalCasters.get(o.getClass())).orElse(bigDecimalUnacceptable).apply(arg.getObj());
            if (o instanceof Boolean) {
                if (connectionProperties.isBooleanToNumber()) {
                    return value;
                }
                throw new SQLException("Cannot cast boolean to BigDecimal");
            }
            return value;
        });

        casters.put(Blob.class, arg -> connectionProperties.asBlob(arg.getObj(), arg.getFrom(), this::getMetaData, arg.getColumnIndex()));
        casters.put(Clob.class, arg -> connectionProperties.asClob(arg.getObj(), arg.getFrom()));
        casters.put(NClob.class, arg -> connectionProperties.asNClob(arg.getObj(), arg.getFrom()));
        casters.put(Date.class, arg -> connectionProperties.asDate(arg.getObj(), arg.getSqlType()));
        casters.put(Time.class, arg -> connectionProperties.asTime(arg.getObj(), arg.getSqlType()));
        casters.put(Timestamp.class, arg -> connectionProperties.asTimestamp(arg.getObj(), arg.getSqlType()));
        casters.put(Boolean.class, arg -> connectionProperties.asBoolean(arg.getObj()));
        casters.put(boolean.class, arg -> connectionProperties.asBoolean(arg.getObj()));
        casters.put(Array.class, arg -> connectionProperties.asArray(arg.getObj()));
    }

    @Override
    public boolean next() throws SQLException {
        return move(format("%s/nextrow", entityUrl), 1);
    }

    @Override
    public void close() throws SQLException {
        connector.delete(format("%s", entityUrl), null, Void.class);
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        return rows == null ? connector.get(format("%s/wasnull", entityUrl), Boolean.class) : wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getString");
        return getValue("index", columnIndex, String.class, columnIndex);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getBoolean");
        return getValue("index", columnIndex, boolean.class, columnIndex);
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getByte");
        return getValue("index", columnIndex, byte.class, columnIndex);
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getShort");
        return getValue("index", columnIndex, short.class, columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getInt");
        return getValue("index", columnIndex, int.class, columnIndex);
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getLong");
        return getValue("index", columnIndex, long.class, columnIndex);
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getFloat");
        return getValue("index", columnIndex, float.class, columnIndex);
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getDouble");
        return getValue("index", columnIndex, double.class, columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        connectionProperties.throwIfUnsupported("getBigDecimal");
        return getValue("index", columnIndex, BigDecimal.class, columnIndex);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getBytes");
        return getValue("index", columnIndex, byte[].class, "bytes", columnIndex);
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getDate");
        return getValue("index", columnIndex, Date.class, columnIndex);
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getTime");
        return getValue("index", columnIndex, Time.class, columnIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getTimestamp");
        return getValue("index", columnIndex, Timestamp.class, columnIndex);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getStream("index", () -> columnIndex, ASCII);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getStream("index", () -> columnIndex, UNICODE);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        return getStream("index", () -> columnIndex, StreamType.BINARY);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getString");
        return getValue("label", columnLabel, String.class, getIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getBoolean");
        return getValue("label", columnLabel, boolean.class, getIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getByte");
        return getValue("label", columnLabel, byte.class, getIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getShort");
        return getValue("label", columnLabel, short.class, getIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getInt");
        return getValue("label", columnLabel, int.class, getIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getLong");
        return getValue("label", columnLabel, long.class, getIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getFloat");
        return getValue("label", columnLabel, float.class, getIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getDouble");
        return getValue("label", columnLabel, double.class, getIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        connectionProperties.throwIfUnsupported("getBigDecimal");
        return getValue("label", columnLabel, BigDecimal.class, getIndex(columnLabel));
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getBytes");
        return getValue("label", columnLabel, byte[].class, "bytes", getIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getDate");
        return getValue("label", columnLabel, Date.class, getIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getTime");
        return getValue("label", columnLabel, Time.class, getIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getTimestamp");
        return getValue("label", columnLabel, Timestamp.class, getIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getStream("label", () -> getIndex(columnLabel), ASCII);
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getStream("label", () -> getIndex(columnLabel), UNICODE);
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getStream("label", () -> getIndex(columnLabel), StreamType.BINARY);
    }

    @Override
    @JsonIgnore
    public SQLWarning getWarnings() throws SQLException {
        connectionProperties.throwIfUnsupported("getWarnings");
        return connector.get(format("%s/warnings", entityUrl), TransportableSQLWarning.class);
    }

    @Override
    public void clearWarnings() throws SQLException {
        connectionProperties.throwIfUnsupported("clearWarnings");
        connector.delete(format("%s/warnings", entityUrl), null, Void.class);
    }

    @Override
    @JsonIgnore
    public String getCursorName() throws SQLException {
        connectionProperties.throwIfUnsupported("getCursorName");
        return connector.get(format("%s/cursorname", entityUrl), String.class);
    }

    @Override
    @JsonIgnore
    public ResultSetMetaData getMetaData() throws SQLException {
        connectionProperties.throwIfUnsupported("getMetaData");
        if (md == null) {
            md = connector.get(format("%s/metadata", entityUrl), TransportableResultSetMetaData.class);
        }
        return md;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        Class clazz = Object.class;
        String className = getMetaData().getColumnClassName(columnIndex);
        if (className != null) {
            clazz = getColumnClass(className);
        }
        return getValue("index", columnIndex, clazz, columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        ResultSetMetaData md = getMetaData();
        int n = md.getColumnCount();
        String columnTypeName = Object.class.getName();
        for (int i = 1; i <= n; i++) {
            if (columnLabel.equals(md.getColumnLabel(i))) {
                columnTypeName = md.getColumnClassName(i);
                break;
            }
        }
        Class<?> clazz = columnTypeName == null ? Object.class : getColumnClass(columnTypeName);
        return getValue("label", columnLabel, clazz, getIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("findColumn");
        return connector.get(format("%s/column/label/%s", entityUrl, encode(columnLabel)), Integer.class);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream("index", () -> columnIndex, false);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream("label", () -> getIndex(columnLabel), false);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getBigDecimal");
        return getValue("index", columnIndex, BigDecimal.class, columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getBigDecimal");
        return getValue("label", columnLabel, BigDecimal.class, getIndex(columnLabel));
    }

    @Override
    @JsonIgnore
    public boolean isBeforeFirst() throws SQLException {
        connectionProperties.throwIfUnsupported("isBeforeFirst");
        return connector.get(format("%s/is/before/first", entityUrl), boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isAfterLast() throws SQLException {
        connectionProperties.throwIfUnsupported("isAfterLast");
        return (rows == null || rows[rows.length - 1].getRow() == null) && connector.get(format("%s/is/after/last", entityUrl), boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isFirst() throws SQLException {
        return isAt("first", "isFirst", RowData::getFirst);
    }

    private boolean isAt(String path, String getter, Function<RowData, Boolean> is) throws SQLFeatureNotSupportedException {
        connectionProperties.throwIfUnsupported(getter);
        if (rows != null) {
            Boolean isAtPosition = is.apply(rows[localRowIndex]);
            if (isAtPosition != null) {
                return isAtPosition;
            }
        }
        return connector.get(format("%s/is/%s", entityUrl, path), Boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isLast() throws SQLException {
        return isAt("last", "isLast", RowData::getLast);
    }

    @Override
    public void beforeFirst() throws SQLException {
        connectionProperties.throwIfUnsupported("beforeFirst");
        moveOutside(format("%s/before/first", entityUrl));
    }

    @Override
    public void afterLast() throws SQLException {
        connectionProperties.throwIfUnsupported("afterLast");
        moveOutside(format("%s/after/last", entityUrl));
    }

    @Override
    public boolean first() throws SQLException {
        connectionProperties.throwIfUnsupported("first");
        return move(format("%s/firstrow", entityUrl), true);
    }

    @Override
    public boolean last() throws SQLException {
        connectionProperties.throwIfUnsupported("last");
        return move(format("%s/lastrow", entityUrl), false);
    }

    @Override
    @JsonIgnore
    public int getRow() throws SQLException {
        connectionProperties.throwIfUnsupported("getRow");
        return connector.get(format("%s/row", entityUrl), int.class);
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        connectionProperties.throwIfUnsupported("absolute");
        return move(format("%s/absoluterow/%d", entityUrl, row), true);
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        connectionProperties.throwIfUnsupported("relative");
        return move(format("%s/relativerow/%d", entityUrl, rows), rows);
    }

    @Override
    public boolean previous() throws SQLException {
        connectionProperties.throwIfUnsupported("previous");
        return move(format("%s/previousrow", entityUrl), -1);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        connectionProperties.throwIfUnsupported("setFetchDirection");
        connector.post(format("%s/fetch/direction", entityUrl), direction, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchDirection() throws SQLException {
        connectionProperties.throwIfUnsupported("getFetchDirection");
        return connector.get(format("%s/fetch/direction", entityUrl), int.class);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        connectionProperties.throwIfUnsupported("setFetchSize");
        connector.post(format("%s/fetch/size", entityUrl), rows, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchSize() throws SQLException {
        connectionProperties.throwIfUnsupported("getFetchSize");
        return connector.get(format("%s/fetch/size", entityUrl), int.class);
    }

    @Override
    @JsonIgnore
    public int getType() throws SQLException {
        connectionProperties.throwIfUnsupported("getType");
        return connector.get(format("%s/type", entityUrl), int.class);
    }

    @Override
    @JsonIgnore
    public int getConcurrency() throws SQLException {
        connectionProperties.throwIfUnsupported("getConcurrency");
        return connector.get(format("%s/concurrency", entityUrl), int.class);
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        connectionProperties.throwIfUnsupported("rowUpdated");
        return connector.get(format("%s/row/updated", entityUrl), boolean.class);
    }

    @Override
    public boolean rowInserted() throws SQLException {
        connectionProperties.throwIfUnsupported("rowInserted");
        return connector.get(format("%s/row/inserted", entityUrl), boolean.class);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        connectionProperties.throwIfUnsupported("rowDeleted");
        return connector.get(format("%s/row/deleted", entityUrl), boolean.class);
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNull");
        super.set(columnIndex, (Class<?>)null, null);
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBoolean");
        set(columnIndex, boolean.class, x);
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateByte");
        set(columnIndex, Byte.class, x);
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateShort");
        set(columnIndex, Short.class, x);
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateInt");
        set(columnIndex, int.class, x);
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateLong");
        set(columnIndex, long.class, x);
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateFloat");
        set(columnIndex, float.class, x);
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateDouble");
        set(columnIndex, double.class, x);
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBigDecimal");
        set(columnIndex, BigDecimal.class, x);
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateString");
        set(columnIndex, String.class, x);
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBytes");
        set(columnIndex, byte[].class, x);
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateDate");
        set(columnIndex, Date.class, x);
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateTime");
        set(columnIndex, Time.class, x);
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateTimestamp");
        set(columnIndex, Timestamp.class, x);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateAsciiStream");
        set(columnIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBinaryStream");
        set(columnIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateCharacterStream");
        set(columnIndex, Reader.class, "CharacterStream", x, length);
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        connectionProperties.throwIfUnsupported("updateObject");
        set(columnIndex, Object.class, x, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateObject");
        set(columnIndex, Object.class, x);
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNull");
        set(columnLabel, (Class<?>)null, null);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBoolean");
        set(columnLabel, boolean.class, x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateByte");
        set(columnLabel, byte.class, x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateShort");
        set(columnLabel, short.class, x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateInt");
        set(columnLabel, int.class, x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateLong");
        set(columnLabel, long.class, x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateFloat");
        set(columnLabel, float.class, x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateDouble");
        set(columnLabel, double.class, x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBigDecimal");
        set(columnLabel, BigDecimal.class, x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateString");
        set(columnLabel, String.class, x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBytes");
        set(columnLabel, byte[].class, x);
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateDate");
        set(columnLabel, Date.class, x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateTime");
        set(columnLabel, Time.class, x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateTimestamp");
        set(columnLabel, Timestamp.class, x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateAsciiStream");
        set(columnLabel, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBinaryStream");
        set(columnLabel, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateCharacterStream");
        set(columnLabel, Reader.class, "CharacterStream", reader, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        connectionProperties.throwIfUnsupported("updateObject");
        set(columnLabel, Object.class, x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateObject");
        set(columnLabel, Object.class, x);
    }

    @Override
    public void insertRow() throws SQLException {
        connectionProperties.throwIfUnsupported("insertRow");
        connector.post(format("%s/row", entityUrl), null, Void.class);
    }

    @Override
    public void updateRow() throws SQLException {
        connectionProperties.throwIfUnsupported("updateRow");
        connector.put(format("%s/row", entityUrl), null, Void.class);
    }

    @Override
    public void deleteRow() throws SQLException {
        connectionProperties.throwIfUnsupported("deleteRow");
        connector.delete(format("%s/row", entityUrl), null, Void.class);
    }

    @Override
    public void refreshRow() throws SQLException {
        connectionProperties.throwIfUnsupported("refreshRow");
        connector.get(format("%s/row", entityUrl), Void.class);
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        connectionProperties.throwIfUnsupported("cancelRowUpdates");
        connector.put(format("%s/row", entityUrl), "cancel", Void.class);
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        connectionProperties.throwIfUnsupported("moveToInsertRow");
        connector.post(format("%s/move", entityUrl), "insert", Void.class);
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        connectionProperties.throwIfUnsupported("moveToCurrentRow");
        connector.post(format("%s/move", entityUrl), "current", Void.class);
    }

    @Override
    @JsonIgnore
    public Statement getStatement() throws SQLException {
        connectionProperties.throwIfUnsupported("getStatement");
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        return null;
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getRef");
        return getValue("index", columnIndex, Ref.class, columnIndex);
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getBlob");
        return getValue("index", columnIndex, Blob.class, columnIndex);
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getClob");
        return getValue("index", columnIndex, Clob.class, columnIndex);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getArray");
        return getValue("index", columnIndex, Array.class, columnIndex);
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        //TODO implement!
        return null;
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getRef");
        return getValue("label", columnLabel, Ref.class, "ref", getIndex(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getBlob");
        return getValue("label", columnLabel, Blob.class, getIndex(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getClob");
        return getValue("label", columnLabel, Clob.class, getIndex(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getArray");
        return getValue("label", columnLabel, Array.class, getIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDateTime(columnIndex, cal, "getDate", "date/index", Date.class, index -> index - 1);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDateTime(columnLabel, cal, "getDate", "date/label", Date.class, this::getIndex);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getDateTime(columnIndex, cal, "getTime", "time/index", Time.class, index -> index - 1);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getDateTime(columnLabel, cal, "getTime", "time/label", Time.class, this::getIndex);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getDateTime(columnIndex, cal, "getTimestamp", "timestamp/index", Timestamp.class, index -> index - 1);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getDateTime(columnLabel, cal, "getTimestamp", "timestamp/label", Timestamp.class, this::getIndex);
    }

    @SuppressWarnings("unchecked")
    private <C, T> T getDateTime(C column, Calendar cal, String getterName, String path, Class<T> type, ThrowingFunction<C, Integer, SQLException> indexGetter) throws SQLException {
        connectionProperties.throwIfUnsupported(getterName);
        return rows == null ? connector.get(format("%s/%s/%s/%s", entityUrl, path, column, calendarParameter(cal)), type) : (T)rows[localRowIndex].getRow()[indexGetter.apply(column)];
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getURL");
        return getValue("index", columnIndex, URL.class, columnIndex);
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getURL");
        return getValue("label", columnLabel, URL.class, getIndex(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateRef");
        set(columnIndex, Ref.class, x);
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateRef");
        set(columnLabel, Ref.class, x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBlob");
        set(columnIndex, Blob.class, x);
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBlob");
        set(columnLabel, Ref.class, x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateClob");
        set(columnIndex, Clob.class, x);
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateClob");
        set(columnLabel, Clob.class, x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateArray");
        set(columnIndex, Array.class, x);
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateArray");
        set(columnLabel, Array.class, x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getRowId");
        return getValue("index", columnIndex, RowId.class, columnIndex);
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getRowId");
        return getValue("label", columnLabel, RowId.class, getIndex(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateRowId");
        set(columnIndex, RowId.class, x);
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateRowId");
        set(columnLabel, RowId.class, x);
    }

    @Override
    @JsonIgnore
    public int getHoldability() throws SQLException {
        connectionProperties.throwIfUnsupported("getHoldability");
        return connector.get(format("%s/holdability", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public boolean isClosed() throws SQLException {
        connectionProperties.throwIfUnsupported("isClosed");
        return closed || connector.get(format("%s/closed", entityUrl), Boolean.class);
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNString");
        set(columnIndex, String.class, "NString", nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNString");
        set(columnLabel, String.class, "NString", nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNClob");
        set(columnIndex, NClob.class, nClob);
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNClob");
        set(columnLabel, NClob.class, nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getNClob");
        return getValue("index", columnIndex, NClob.class, columnIndex);
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getNClob");
        return getValue("label", columnLabel, NClob.class, getIndex(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getSQLXML");
        return getValue("index", columnIndex, SQLXML.class, columnIndex);
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getSQLXML");
        return getValue("label", columnLabel, SQLXML.class, getIndex(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        connectionProperties.throwIfUnsupported("updateSQLXML");
        set(columnIndex, SQLXML.class, xmlObject);
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        connectionProperties.throwIfUnsupported("updateSQLXML");
        set(columnLabel, SQLXML.class, xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getNString");
        return getValue("index", columnIndex, String.class, "nstring", columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getNString");
        return getValue("label", columnLabel, String.class, "nstring", getIndex(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream("index", () -> columnIndex, true);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream("label", () -> getIndex(columnLabel), true);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNCharacterStream");
        set(columnIndex, Reader.class, "NCharacterStream", x, length);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNCharacterStream");
        set(columnLabel, Reader.class, "NCharacterStream", reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateAsciiStream");
        set(columnIndex, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBinaryStream");
        set(columnIndex, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateCharacterStream");
        set(columnIndex, Reader.class, "CharacterStream", x, length);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateAsciiStream");
        set(columnLabel, InputStream.class, "AsciiStream", x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBinaryStream");
        set(columnLabel, InputStream.class, "BinaryStream", x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateCharacterStream");
        set(columnLabel, InputStream.class, "CharacterStream", reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBlob");
        set(columnIndex, Blob.class, inputStream, length);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBlob");
        set(columnLabel, Blob.class, inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateClob");
        set(columnIndex, Clob.class, reader, length);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateClob");
        set(columnLabel, Clob.class, reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNClob");
        set(columnIndex, NClob.class, reader, length);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNClob");
        set(columnLabel, NClob.class, reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNCharacterStream");
        set(columnIndex, Reader.class, "NCharacterStream", x);
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNCharacterStream");
        set(columnLabel, Reader.class, "NCharacterStream", reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateAsciiStream");
        set(columnIndex, InputStream.class, "AsciiStream", x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBinaryStream");
        set(columnIndex, InputStream.class, "BinaryStream", x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateCharacterStream");
        set(columnIndex, Reader.class, "CharacterStream", x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateAsciiStream");
        set(columnLabel, InputStream.class, "AsciiStream", x);
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBinaryStream");
        set(columnLabel, InputStream.class, "BinaryStream", x);
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        connectionProperties.throwIfUnsupported("updateCharacterStream");
        set(columnLabel, Reader.class, "CharacterStream", reader);
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBlob");
        set(columnIndex, Blob.class, inputStream);
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        connectionProperties.throwIfUnsupported("updateBlob");
        set(columnLabel, Blob.class, inputStream);
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        connectionProperties.throwIfUnsupported("updateClob");
        set(columnIndex, Clob.class, reader);
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        connectionProperties.throwIfUnsupported("updateClob");
        set(columnLabel, Clob.class, reader);
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNClob");
        set(columnIndex, NClob.class, reader);
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        connectionProperties.throwIfUnsupported("updateNClob");
        set(columnLabel, NClob.class, reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        return rows == null ? connector.get(format("%s/object/index/%d/%s", entityUrl, columnIndex, type), type) : cast(rows[localRowIndex].getRow()[columnIndex - 1], getClassOfColumn(columnIndex), type, columnIndex);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        return rows == null ? connector.get(format("%s/object/label/%s/%s", entityUrl, columnLabel, type), type) : cast(rows[localRowIndex].getRow()[getIndex(columnLabel)], getClassOfColumn(columnLabel), type, getDataOfColumnIndex(columnLabel));
    }

    public ResultSetProxy withStatement(Statement statement) {
        this.statement = statement;
        return this;
    }

    private int getDataOfColumnIndex(String columnLabel) throws SQLException {
        ResultSetMetaData md = getMetaData();
        int n = md.getColumnCount();
        for (int i = 1; i <= n; i++) {
            if (columnLabel.equals(md.getColumnLabel(i))) {
                return i;
            }
        }
        throw new SQLException(format("Column %s does not exist", columnLabel));
    }

    private <T> T getDataOfColumn(String columnLabel, ThrowingBiFunction<ResultSetMetaData, Integer, T, SQLException> getter) throws SQLException {
        ResultSetMetaData md = getMetaData();
        int n = md.getColumnCount();
        for (int i = 1; i <= n; i++) {
            if (columnLabel.equals(md.getColumnLabel(i))) {
                return getter.apply(md, i);
            }
        }
        throw new SQLException(format("Column %s does not exist", columnLabel));
    }

    private Class<?> getClassOfColumn(String columnLabel) throws SQLException {
        return getDataOfColumn(columnLabel, (md, i) -> getColumnClass(md.getColumnClassName(i)));
    }

    private Class<?> getClassOfColumn(int columnIndex) throws SQLException {
        String className = getMetaData().getColumnClassName(columnIndex);
        return className == null ? Object.class : getColumnClass(className);
    }

    private String getTypeNameOfColumn(int columnIndex) throws SQLException {
        return getMetaData().getColumnTypeName(columnIndex);
    }

    private String calendarParameter(Calendar cal) {
        // TODO: try to add some support of Locale: it can be passed to constructor but is not stored as-is in Calendar: parts are used instead
        return format("tz=%s,millis=%d", cal.getTimeZone().getID(), cal.getTimeInMillis());
    }

    private <T, M> T getValue(String markerName, M columnMarker, Class<T> clazz, Integer columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("get" + clazz.getSimpleName());
        return getValue(markerName, columnMarker, clazz, clazz.getSimpleName().toLowerCase(), columnIndex);
    }

    private <T, M> T getValue(String markerName, M columnMarker, Class<T> clazz, String typeName, Integer columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("get" + clazz.getSimpleName());
        return rows == null || columnIndex == null ?
                connector.get(format("%s/%s/%s/%s", entityUrl, typeName, markerName, columnMarker), clazz) :
                cast(rows[localRowIndex].getRow()[columnIndex - 1], getClassOfColumn(columnIndex), clazz, columnIndex);
    }

    private Integer getIndex(String columnLabel) throws SQLException {
        return ((TransportableResultSetMetaData)getMetaData()).getIndex(columnLabel);
    }

    @SuppressWarnings("unchecked")
    private <T> T cast(Object obj, Class<?> from, Class<T> to, int columnIndex) throws SQLException {
        String typeName = getTypeNameOfColumn(columnIndex);
        wasNull = false;
        if (obj != null && to.isAssignableFrom(obj.getClass())) {
            return (T)obj;
        }
        ThrowingFunction<CastorArg, ?, SQLException> caster = casters.get(to);
        if (caster != null) {
            try {
                CastorArg arg = new CastorArg(obj, from, to, getMetaData().getColumnType(columnIndex), columnIndex);
                return (T) caster.apply(arg);
            } catch (IllegalArgumentException e) {
                throw new SQLException(e.getMessage());
            }
        } else if (Ref.class.equals(to)) {
            return (T)new TransportableRef(obj, typeName);
        } else if (obj == null) {
            wasNull = true;
            return (T)defaultValues.get(to);
        }
        if (String.class.equals(to)) {
            return (T)connectionProperties.asString(obj, this::getMetaData, columnIndex);
        }
        //return objectMapper.readValue(obj instanceof String ? "\"" + obj + "\"" : "" + obj, clazz);
        return (T)obj;
    }

    private String toBooleanString(boolean b) {
        return connectionProperties.toBooleanString(b);
    }

    private boolean move(String url, int delta) throws SQLException {
        int index = localRowIndex + delta;
        if (rows != null && index >= 0 && index < rows.length) {
            localRowIndex = index;
            return rows[localRowIndex].isMoved();
        }
        return move(url, delta > 0);
    }

    private boolean move(String url, boolean moveToFirst) {
        rows = connector.get(url, RowData[].class);
        wasNull = false;
        localRowIndex = moveToFirst ? 0 : rows.length - 1;
        return rows.length > 0 && rows[localRowIndex].isMoved();
    }

    public void moveOutside(String url) throws SQLException {
        rows = null;
        wasNull = false;
        connector.post(url, null, Void.class);
    }

    private static boolean inRange(Object n, long min, long max) {
        return inRange(n, min, max, Number::longValue);
    }

    private static boolean inRange(Object n, double min, double max) {
        return inRange(n, min, max, Number::doubleValue);
    }

    private static <T extends Number & Comparable<T>> boolean inRange(Object n, T min, T max, Function<Number, T> toSpecificNumber) {
        if (n == null) {
            return true;
        }
        if (n instanceof Number) {
            T value = toSpecificNumber.apply((Number)n);
            return value.compareTo(min) > 0 && value.compareTo(max) <= 0;
        }
        return n instanceof Boolean;
    }

    private Class<?> getColumnClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            return Object.class;
        }
    }

    private Reader getCharacterStream(String markerName, ThrowingSupplier<Integer, SQLException> columnIndexSupplier, boolean n) throws SQLException {
        connectionProperties.throwIfUnsupported("get" + (n ? "N" : "") + "CharacterStream");
        int columnIndex = columnIndexSupplier.get();
        String path = (n ? "n" : "") + "character/stream";
        return rows == null ?
                connector.get(format("%s/%s/%s/%s", entityUrl, path, markerName, columnIndex), Reader.class) :
                connectionProperties.asReader(rows[localRowIndex].getRow()[columnIndex - 1], getClassOfColumn(columnIndex), this::getMetaData, columnIndex, n);
    }

    private InputStream getStream(String markerName, ThrowingSupplier<Integer, SQLException> columnIndexSupplier, StreamType streamType) throws SQLException {
        String streamTypeName = streamType.name();
        connectionProperties.throwIfUnsupported("get" + streamTypeName.substring(0, 1).toUpperCase() + streamTypeName.substring(1) + "Stream");
        int columnIndex = columnIndexSupplier.get();
        return rows == null ?
                connector.get(format("%s/%s/%s/%s", entityUrl, streamType + "/stream", markerName, columnIndex), InputStream.class) :
                streamType.asStream(connectionProperties, rows[localRowIndex].getRow()[columnIndex - 1], getClassOfColumn(columnIndex), this::getMetaData, columnIndex);
    }
}

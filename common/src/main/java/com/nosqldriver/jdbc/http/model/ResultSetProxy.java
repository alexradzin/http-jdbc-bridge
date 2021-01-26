package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingFunction;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.jdbc.http.Util.encode;
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

    private static final Map<Class<?>, ThrowingFunction<CastorArg, ? extends Object, SQLException>> generalCastors;

    static {
        Map<Class<?>, ThrowingFunction<CastorArg, ? extends Object, SQLException>> map = new HashMap<>();
        for (SimpleEntry<Class<?>, ? extends ThrowingFunction<CastorArg, ? extends Object, SQLException>> classSimpleEntry : Arrays.asList(
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Byte, SQLException>>(Byte.class, new NumericCastor<>(e -> inRange(e, Byte.MIN_VALUE, Byte.MAX_VALUE), e -> (byte)Math.round(((Number) e).doubleValue()), b -> (byte)(b ? 1 : 0), Byte.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Byte, SQLException>>(byte.class, new NumericCastor<>(e -> inRange(e, Byte.MIN_VALUE, Byte.MAX_VALUE), e -> (byte)Math.round(((Number) e).doubleValue()), b -> (byte)(b ? 1 : 0), byte.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Short, SQLException>>(Short.class, new NumericCastor<>(e -> inRange(e, Short.MIN_VALUE, Short.MAX_VALUE), e -> (short)Math.round(((Number) e).doubleValue()), s -> (short)(s ? 1 : 0), Short.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Short, SQLException>>(short.class, new NumericCastor<>(e -> inRange(e, Short.MIN_VALUE, Short.MAX_VALUE), e -> (short)Math.round(((Number) e).doubleValue()), s -> (short)(s ? 1 : 0), short.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Integer, SQLException>>(Integer.class, new NumericCastor<>(e -> inRange(e, Integer.MIN_VALUE, Integer.MAX_VALUE), e -> (int)Math.round(((Number) e).doubleValue()), i -> (i ? 1 : 0), Integer.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Integer, SQLException>>(int.class, new NumericCastor<>(e -> inRange(e, Integer.MIN_VALUE, Integer.MAX_VALUE), e -> (int)Math.round(((Number) e).doubleValue()), i -> (i ? 1 : 0), int.class)),

                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Long, SQLException>>(Long.class, new NumericCastor<>(e -> inRange(e, Long.MIN_VALUE, Long.MAX_VALUE), e -> Math.round(((Number) e).doubleValue()), i -> (i ? 1L : 0L), Long.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Long, SQLException>>(long.class, new NumericCastor<>(e -> inRange(e, Long.MIN_VALUE, Long.MAX_VALUE), e -> Math.round(((Number) e).doubleValue()), i -> (i ? 1L : 0L), long.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Object, SQLException>>(BigDecimal.class, arg -> arg.getObj() == null ? null : Optional.ofNullable(bigDecimalCasters.get(arg.getObj().getClass())).orElse(bigDecimalUnacceptable).apply(arg.getObj())),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Float, SQLException>>(Float.class, new NumericCastor<>(e -> inRange(e, -Float.MAX_VALUE, Float.MAX_VALUE), e -> ((Number) e).floatValue(), f -> (f ? 1.f : 0.f), Float.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Float, SQLException>>(float.class, new NumericCastor<>(e -> inRange(e, -Float.MAX_VALUE, Float.MAX_VALUE), e -> ((Number) e).floatValue(), f -> (f ? 1.f : 0.f), float.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Double, SQLException>>(Double.class, new NumericCastor<>(e -> inRange(e, -Double.MAX_VALUE, Double.MAX_VALUE), e -> ((Number) e).doubleValue(), f -> (f ? 1. : 0.), Double.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, Double, SQLException>>(double.class, new NumericCastor<>(e -> inRange(e, -Double.MAX_VALUE, Double.MAX_VALUE), e -> ((Number) e).doubleValue(), f -> (f ? 1. : 0.), double.class)),
                new SimpleEntry<Class<?>, ThrowingFunction<CastorArg, ? extends Array, SQLException>>(Array.class, arg -> new ProxyFactory<>(ArrayProxy.class, o -> o == null || o instanceof Array ? (Array)o : new TransportableArray(null, 0, new Object[]{o})).apply(arg.getObj()))
        )) {
            if (map.put(classSimpleEntry.getKey(), classSimpleEntry.getValue()) != null) {
                throw new IllegalStateException("Duplicate key");
            }
        }
        generalCastors = map;
    }

    private static class CastorArg {
        private final Object obj;
        private final Class<?> from;
        private final Class<?> to;
        private final int sqlType;

        public CastorArg(Object obj, Class<?> from, Class<?> to, int sqlType) {
            this.obj = obj;
            this.from = from;
            this.to = to;
            this.sqlType = sqlType;
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
    }

    private final Map<Class<?>, ThrowingFunction<CastorArg, ? extends Object, SQLException>> casters;

    private static class NumericCastor<T> implements ThrowingFunction<CastorArg, T, SQLException> {
        private final Predicate<Object> checker;
        private final Function<Object, T> actualCastor;
        private final Function<Boolean, T> booleanCastor;
        private final Class<T> type;

        private NumericCastor(Predicate<Object> checker, Function<Object, T> actualCastor, Function<Boolean, T> booleanCastor, Class<T> type) {
            this.checker = checker;
            this.actualCastor = actualCastor;
            this.booleanCastor = booleanCastor;
            this.type = type;
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
    private volatile RowData rowData = null;
    private volatile boolean wasNull = false;

    // This constructor is temporary patch
//    @JsonCreator
    public ResultSetProxy(@JsonProperty("entityUrl") String entityUrl) {
        this(entityUrl, new ConnectionProperties(System.getProperties()));
    }

    @JsonCreator
    public ResultSetProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("connectionProperties") ConnectionProperties connectionProperties) {
        super(entityUrl, ResultSet.class);
        this.connectionProperties = connectionProperties;
        casters = generalCastors;

        casters.put(byte.class, new NumericCastor<>(e -> inRange(e, Byte.MIN_VALUE, Byte.MAX_VALUE), e -> (byte)connectionProperties.toInteger(((Number) e).doubleValue()), b -> (byte)(b ? 1 : 0), Byte.class));
        casters.put(short.class, new NumericCastor<>(e -> inRange(e, Short.MIN_VALUE, Short.MAX_VALUE), e -> (short)connectionProperties.toInteger(((Number) e).doubleValue()), b -> (short)(b ? 1 : 0), Short.class));
        casters.put(int.class, new NumericCastor<>(e -> inRange(e, Integer.MIN_VALUE, Integer.MAX_VALUE), e -> (int)connectionProperties.toInteger(((Number) e).doubleValue()), b -> (int)(b ? 1 : 0), Integer.class));
        casters.put(long.class, new NumericCastor<>(e -> inRange(e, Long.MIN_VALUE, Long.MAX_VALUE), e -> connectionProperties.toInteger(((Number) e).doubleValue()), b -> (long)(b ? 1 : 0), Long.class));
        casters.put(Byte.class, new NumericCastor<>(e -> inRange(e, Byte.MIN_VALUE, Byte.MAX_VALUE), e -> (byte)connectionProperties.toInteger(((Number) e).doubleValue()), b -> (byte)(b ? 1 : 0), Byte.class));
        casters.put(Short.class, new NumericCastor<>(e -> inRange(e, Short.MIN_VALUE, Short.MAX_VALUE), e -> (short)connectionProperties.toInteger(((Number) e).doubleValue()), b -> (short)(b ? 1 : 0), Short.class));
        casters.put(Integer.class, new NumericCastor<>(e -> inRange(e, Integer.MIN_VALUE, Integer.MAX_VALUE), e -> (int)connectionProperties.toInteger(((Number) e).doubleValue()), b -> (int)(b ? 1 : 0), Integer.class));
        casters.put(Long.class, new NumericCastor<>(e -> inRange(e, Long.MIN_VALUE, Long.MAX_VALUE), e -> connectionProperties.toInteger(((Number) e).doubleValue()), b -> (long)(b ? 1 : 0), Long.class));
        casters.put(Blob.class, arg -> connectionProperties.asBlob(arg.getObj(), arg.getFrom()));
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
        return move(format("%s/nextrow", entityUrl));
    }

    @Override
    public void close() throws SQLException {
        connector.delete(format("%s", entityUrl), null, Void.class);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return rowData == null ? connector.get(format("%s/wasnull", entityUrl), Boolean.class) : wasNull;
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
        connectionProperties.throwIfUnsupported("getAsciiStream");
        return getValue("index", columnIndex, InputStream.class, "ascii/stream", columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getUnicodeStream");
        return getValue("index", columnIndex, InputStream.class, "unicode/stream", columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getBinaryStream");
        return getValue("index", columnIndex, InputStream.class, "binary/stream", columnIndex);
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
        connectionProperties.throwIfUnsupported("getAsciiStream");
        return getValue("label", columnLabel, InputStream.class, "ascii/stream", getIndex(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getUnicodeStream");
        return getValue("label", columnLabel, InputStream.class, "unicode/stream", getIndex(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getBinaryStream");
        return getValue("label", columnLabel, InputStream.class, "binary/stream", getIndex(columnLabel));
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
        return getValue("index", columnIndex, getColumnClass(getMetaData().getColumnClassName(columnIndex)), columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        ResultSetMetaData md = getMetaData();
        int n = md.getColumnCount();
        String columnTypeName = Object.class.getName();
        for (int i = 1; i <= n; i++) {
            if (columnLabel.equals(md.getColumnLabel(i))) {
                columnTypeName = md.getColumnTypeName(i);
                break;
            }
        }
        return getValue("label", columnLabel, getColumnClass(columnTypeName), getIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("findColumn");
        return connector.get(format("%s/column/label/%s", entityUrl, encode(columnLabel)), Integer.class);
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        connectionProperties.throwIfUnsupported("getCharacterStream");
        return getValue("index", columnIndex, Reader.class, "character/stream", columnIndex);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getCharacterStream");
        return getValue("label", columnLabel, Reader.class, "character/stream", getIndex(columnLabel));
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
        return connector.get(format("%s/is/after/last", entityUrl), boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isFirst() throws SQLException {
        connectionProperties.throwIfUnsupported("isFirst");
        return connector.get(format("%s/is/first", entityUrl), boolean.class);
    }

    @Override
    @JsonIgnore
    public boolean isLast() throws SQLException {
        connectionProperties.throwIfUnsupported("isLast");
        return connector.get(format("%s/is/last", entityUrl), Boolean.class);
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
        return move(format("%s/firstrow", entityUrl));
    }

    @Override
    public boolean last() throws SQLException {
        connectionProperties.throwIfUnsupported("last");
        return move(format("%s/lastrow", entityUrl));
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
        return move(format("%s/absoluterow/%d", entityUrl, row));
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        connectionProperties.throwIfUnsupported("relative");
        return move(format("%s/relativerow/%d", entityUrl, rows));
    }

    @Override
    public boolean previous() throws SQLException {
        connectionProperties.throwIfUnsupported("previous");
        return move(format("%s/previousrow", entityUrl));
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
        connectionProperties.throwIfUnsupported("getDate");
        return rowData == null ? connector.get(format("%s/date/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Date.class) : (Date)rowData.getRow()[columnIndex - 1];
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        connectionProperties.throwIfUnsupported("getDate");
        return rowData == null ? connector.get(format("%s/date/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Date.class) : (Date)rowData.getRow()[getIndex(columnLabel)];
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        connectionProperties.throwIfUnsupported("getTime");
        return rowData == null ? connector.get(format("%s/time/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Time.class) : (Time)rowData.getRow()[columnIndex - 1];
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        connectionProperties.throwIfUnsupported("getTime");
        return rowData == null ? connector.get(format("%s/time/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Time.class) : (Time)rowData.getRow()[getIndex(columnLabel)];
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        connectionProperties.throwIfUnsupported("getTimestamp");
        return rowData == null ? connector.get(format("%s/timestamp/index/%s/%s", entityUrl, columnIndex, calendarParameter(cal)), Timestamp.class) : (Timestamp)rowData.getRow()[columnIndex - 1];
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        connectionProperties.throwIfUnsupported("getTimestamp");
        return rowData == null ? connector.get(format("%s/timestamp/label/%s/%s", entityUrl, columnLabel, calendarParameter(cal)), Timestamp.class) : (Timestamp)rowData.getRow()[getIndex(columnLabel)];
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
        return connector.get(format("%s/closed", entityUrl), Boolean.class);
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
        connectionProperties.throwIfUnsupported("getNCharacterStream");
        return getValue("index", columnIndex, Reader.class, "ncharacter/stream", columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        connectionProperties.throwIfUnsupported("getNCharacterStream");
        return getValue("label", columnLabel, Reader.class, "ncharacter/stream", getIndex(columnLabel));
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
    @SuppressWarnings("unchecked")
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        return rowData == null ? connector.get(format("%s/object/index/%d/%s", entityUrl, columnIndex, type), type) : cast(rowData.getRow()[columnIndex - 1], getClassOfColumn(columnIndex), type, columnIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        connectionProperties.throwIfUnsupported("getObject");
        return rowData == null ? connector.get(format("%s/object/label/%s/%s", entityUrl, columnLabel, type), type) : cast(rowData.getRow()[getIndex(columnLabel)], getClassOfColumn(columnLabel), type, getDataOfColumnIndex(columnLabel));
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

    private String getTypeNameOfColumn(String columnLabel) throws SQLException {
        return getDataOfColumn(columnLabel, (md, i) -> getTypeNameOfColumn(i));
    }

    private Class<?> getClassOfColumn(int columnIndex) throws SQLException {
        return getColumnClass(getMetaData().getColumnClassName(columnIndex));
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
        return rowData == null || columnIndex == null ?
                connector.get(format("%s/%s/%s/%s", entityUrl, typeName, markerName, columnMarker), clazz) :
                cast(rowData.getRow()[columnIndex - 1], getClassOfColumn(columnIndex), clazz, columnIndex);
    }

    private Integer getIndex(String columnLabel) throws SQLException {
        return ((TransportableResultSetMetaData)getMetaData()).getIndex(columnLabel);
    }

    private <T> T cast(Object obj, Class<?> from, Class<T> to, int columnIndex) throws SQLException {
        String typeName = getTypeNameOfColumn(columnIndex);
        wasNull = false;
        if (obj != null && to.isAssignableFrom(obj.getClass())) {
            return (T)obj;
        }
        ThrowingFunction<CastorArg, ?, SQLException> caster = casters.get(to);
        //noinspection unchecked
        //try {
            if (caster != null) {
                try {
                    CastorArg arg = new CastorArg(obj, from, to, getMetaData().getColumnType(columnIndex));
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
                if (obj instanceof String) {
                    return (T)obj;
                }
                if (obj instanceof Boolean) {
                    return (T)(toBooleanString((boolean)obj));
                }
                if (obj instanceof Timestamp) {
                    return (T)connectionProperties.asString((Timestamp)obj, getMetaData().getColumnType(columnIndex));
                }
                if (obj instanceof Array) {
                    Object a = ((Array)obj).getArray();
                    int n = java.lang.reflect.Array.getLength(a);
                    List<Object> list = new ArrayList<>(n);
                    for (int i = 0; i < n; i++) {
                        list.add(java.lang.reflect.Array.get(a, i));
                    }
                    return (T)list.toString();
                }

                return (T)connectionProperties.asString(obj);
            }
            //return objectMapper.readValue(obj instanceof String ? "\"" + obj + "\"" : "" + obj, clazz);
            return (T)obj;
        //} catch (JsonProcessingException e) {
        //    throw new IllegalArgumentException(e);
        //}
    }

    private String toBooleanString(boolean b) {
        return connectionProperties.toBooleanString(b);
    }

    private boolean move(String url) throws SQLException {
        rowData = connector.get(url, RowData.class);
        wasNull = false;
        return rowData.isMoved();
    }

    public void moveOutside(String url) throws SQLException {
        rowData = null;
        wasNull = false;
        connector.post(url, null, Void.class);
    }

    private static boolean inRange(Object n, long min, long max) {
        if (n == null) {
            return true;
        }
        if (n instanceof Number) {
            long l = ((Number) n).longValue();
            return l >= min && l <= max;
        } else if (n instanceof Boolean) {
            return true;
        }
        return false;
    }

    private static boolean inRange(Object n, double min, double max) {
        if (n instanceof Number) {
            double d = ((Number) n).doubleValue();
            return d >= min && d <= max;
        } else if (n instanceof Boolean) {
            return true;
        }
        return false;
    }

    private Class<?> getColumnClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            return Object.class;
        }
    }
}

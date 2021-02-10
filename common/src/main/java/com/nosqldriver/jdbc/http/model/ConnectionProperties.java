package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.util.function.ThrowingFunction;
import com.nosqldriver.util.function.ThrowingSupplier;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class ConnectionProperties {
    Map<Class<?>, Function<Object, byte[]>> toBytes =
            Stream.of(
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Boolean.class, o -> ByteBuffer.allocate(1).put((byte)((boolean)o ? 1 : 0)).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(String.class, o -> ((String)o).getBytes()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Float.class, i -> ByteBuffer.allocate(4).putFloat(((Number) i).floatValue()).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Double.class, i -> ByteBuffer.allocate(8).putDouble(((Number) i).doubleValue()).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Character.class, i -> ByteBuffer.allocate(2).putChar(((Character) i)).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Byte.class, i -> ByteBuffer.allocate(1).put(((Number) i).byteValue()).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Short.class, i -> ByteBuffer.allocate(2).putShort(((Number) i).shortValue()).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Integer.class, i -> ByteBuffer.allocate(4).putInt(((Number) i).intValue()).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(Long.class, i -> ByteBuffer.allocate(8).putLong(((Number) i).longValue()).array()),
                    new SimpleEntry<Class<?>, Function<Object, byte[]>>(BigDecimal.class, i -> ByteBuffer.allocate(8).putDouble(((Number) i).doubleValue()).array())
            ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    Map<Integer, Class<?>> sqlTypeToClass = Stream.of(new SimpleEntry<Integer, Class<?>>(Types.BIT, Byte.class))
            .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    private static final Function<Object, byte[]> serializableToBytes = obj -> {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            // will throw exception if obj is not Serializable
            oos.writeObject(obj);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    };


    private static final Map<String, Class> primitives =
            Stream.of(
                    byte.class, short.class, int.class, long.class, boolean.class, float.class, double.class,
                    Byte.class, Short.class, Integer.class, Long.class, Boolean.class, Float.class, Double.class)
            .collect(Collectors.toMap(Class::getSimpleName, c -> c));

    private static final Map<Class<?>, Class<?>> types =
            Stream.of(
                    new SimpleEntry<>(byte.class, Byte.class),
                    new SimpleEntry<>(short.class, Short.class),
                    new SimpleEntry<>(int.class, Integer.class),
                    new SimpleEntry<>(long.class, Long.class),
                    new SimpleEntry<>(float.class, Float.class),
                    new SimpleEntry<>(double.class, Double.class),
                    new SimpleEntry<>(boolean.class, Boolean.class),
                    new SimpleEntry<>(Byte.class, byte.class),
                    new SimpleEntry<>(Short.class, short.class),
                    new SimpleEntry<>(Integer.class, int.class),
                    new SimpleEntry<>(Long.class, long.class),
                    new SimpleEntry<>(Float.class, float.class),
                    new SimpleEntry<>(Double.class, double.class),
                    new SimpleEntry<>(Boolean.class, boolean.class),
                    new SimpleEntry<>(BigDecimal.class, BigDecimal.class)
            ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    enum NumericToBoolean implements Function<Object, Boolean> {
        NE0 {
            @Override
            public Boolean apply(Object n) {
                return ((Number)n).doubleValue() != 0;
            }
        },
        GT0 {
            @Override
            public Boolean apply(Object n) {
                return ((Number)n).doubleValue() > 0;
            }
        }
    }

    private final Map<Class, Function<Object, Boolean>> toBooleanCastors;

    private final boolean timestampWithMillis; //= true;
    private final boolean anyClob;// = true;
    private final boolean anyNClob;// = true;
    private final boolean anyArray;// = true;
    private final boolean anyArrayPartial;// = false;
    private final boolean charToByte;
    private final boolean booleanToNumber;
    private final Map<String, LobProperties> toBlob;// = true;
    private final Collection<String> stringBlob;
    private final Round floatToInt;// = Round.INT; // h2 -> floor
    private final Map<Boolean, String> booleanLiterals;
    private final Collection<String> unsupportedFunctions;
    private final Map<Class, TimeDataProperties> toTimestamp;// = true;
    private final Map<Class, TimeDataProperties> toDate;
    private final Map<Class, TimeDataProperties> toTime;// = true;
    private final Collection<Class> toBoolean;// = true;
    private final Map<Class, String> stringFormats;
    private final TimeZone timezone;

    enum Round {
        CEIL {
            @Override
            long getInt(double value) {
                return (long)Math.ceil(value);
            }
        },
        FLOOR {
            @Override
            long getInt(double value) {
                return (long)Math.floor(value);
            }
        },
        ROUND {
            @Override
            long getInt(double value) {
                return Math.round(value);
            }
        },
        INT {
            @Override
            long getInt(double value) {
                return (long)value;
            }
        },;

        abstract long getInt(double value);
    }

    private final @JsonProperty("props") Properties props;

    @JsonCreator
    public ConnectionProperties(@JsonProperty("props") Properties props) {
        this.props = props;
        this.timestampWithMillis = getBoolean(props, "timestampWithMillis", true);
        this.anyClob = getBoolean(props, "anyClob", true);
        this.anyNClob = getBoolean(props, "anyNClob", true);
        this.anyArray = getBoolean(props, "anyArray", true);
        this.anyArrayPartial = getPartialBoolean(props, "anyArray");
        this.charToByte = getBoolean(props, "charToByte", false);
        this.booleanToNumber = getBoolean(props, "booleanToNumber", false);
        toBlob = getPropertyValue(props, "toBlob", this::createLobProperties, Collectors.toMap(LobProperties::getClassName, e -> e), emptyMap());
        stringBlob = getPropertyValue(props, "stringBlob", typeName -> Optional.ofNullable(primitives.get(typeName)).map(types::get).map(Class::getName).orElse(typeName), toSet(), emptySet());

        this.floatToInt = Round.valueOf(props.getProperty("floatToInt", Round.INT.name()));
        String[] boolLiterals = props.getProperty("booleanLiterals", "FALSE,TRUE").split("\\s*,\\s*");
        this.booleanLiterals = new HashMap<>();
        booleanLiterals.put(false, boolLiterals[0]);
        booleanLiterals.put(true, boolLiterals[1]);
        unsupportedFunctions = new HashSet<>(Arrays.asList(props.getProperty("unsupportedFunctions", "").split("\\s*,\\s*")));
        toTimestamp = getTimePropertiesMap(props, "toTimestamp");
        toTime = getTimePropertiesMap(props, "toTime");
        toDate = getTimePropertiesMap(props, "toDate");

        toBoolean = Stream.concat(
                Stream.of(boolean.class, Boolean.class),
                Optional.ofNullable(props.getProperty("toBoolean")).map(p -> Arrays.stream(p.split("\\s*,\\s*"))
                        .map(typeName -> Optional.ofNullable(primitives.get(typeName)).orElseGet(() -> toClass(typeName)))).orElse(Stream.empty())
        ).collect(Collectors.toList());

        stringFormats = props.entrySet().stream().filter(e -> ((String)e.getKey()).startsWith("asString.")).map(e -> {
            String typeName = ((String) e.getKey()).substring("asString.".length());
            Class<?> type = Optional.ofNullable(primitives.get(typeName)).orElseGet(() -> toClass(typeName));
            String format = (String)e.getValue();
            return new SimpleEntry<>(type, format);
        }).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

        timezone = Optional.ofNullable(props.getProperty("timezone"))
                .map(tzid -> {TimeZone tz = TimeZone.getTimeZone(tzid); tz.setRawOffset(2 * (TimeZone.getDefault().getRawOffset() - tz.getRawOffset()));  return tz;})
                .orElseGet(TimeZone::getDefault);


        Function<Object, Boolean> numberToBoolean = NumericToBoolean.valueOf(props.getProperty("numberToBoolean", NumericToBoolean.NE0.name()));
        toBooleanCastors = Stream.of(
                new SimpleEntry<Class, Function<Object, Boolean>>(Object.class, Objects::nonNull),
                new SimpleEntry<Class, Function<Object, Boolean>>(String.class, Objects::nonNull),
                new SimpleEntry<Class, Function<Object, Boolean>>(byte.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(short.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(int.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(long.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(float.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(double.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(boolean.class, b -> (boolean)b),
                new SimpleEntry<Class, Function<Object, Boolean>>(Byte.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(Short.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(Integer.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(Long.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(Float.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(Double.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(BigDecimal.class, numberToBoolean),
                new SimpleEntry<Class, Function<Object, Boolean>>(Boolean.class, b -> (Boolean)b)
        ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    }

    private static boolean getBoolean(Properties props, String name, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(name, "" + defaultValue).replace(".", ""));
    }

    private static boolean getPartialBoolean(Properties props, String name) {
        return Optional.ofNullable(props.getProperty(name)).map(p -> p.endsWith(".")).orElse(false);
    }

    public long toInteger(double value) {
        return floatToInt.getInt(value);
    }

    public byte toByte(char c) {
        return (byte)c;
    }

    private static final DateFormat timestampWithTimezone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ"); // 2020-10-08 19:18:17+00
    static {
        timestampWithTimezone.setTimeZone(TimeZone.getTimeZone("UTC")); // TODO: probably get it from the current timezone of the DB
    }

    public String asString(Object obj, ThrowingSupplier<ResultSetMetaData, SQLException> md, int columnIndex) throws SQLException {
        if (obj instanceof String) {
            return (String)obj;
        }
        if (obj instanceof Boolean) {
            return (toBooleanString((boolean)obj));
        }
        if (obj instanceof Timestamp) {
            return asString((Timestamp)obj, md.get().getColumnType(columnIndex));
        }
        if (obj instanceof Array) {
            Object a = ((Array)obj).getArray();
            int n = java.lang.reflect.Array.getLength(a);
            List<Object> list = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                list.add(java.lang.reflect.Array.get(a, i));
            }
            return list.toString();
        }

        return asFormattedString(obj);

    }

    public String asString(Timestamp ts, int type) {
        if (Types.TIMESTAMP_WITH_TIMEZONE == type) {
            return timestampWithTimezone.format(ts).replaceFirst("00$", "");
        }

        String str = ts.toString();
        return timestampWithMillis ? str : str.replaceFirst("\\.\\d+$", "");
    }

    public <T> Blob asBlob(T obj, Class fromClazz, ThrowingSupplier<ResultSetMetaData, SQLException> md, int columnIndex) throws SQLException {
        String fromClassName = fromClazz == null ? null : fromClazz.getName();

        LobProperties lobProps = toBlob.get(fromClassName);
        if (lobProps == null) {
            Class refClass = types.get(fromClazz);
            if (refClass != null) {
                lobProps = toBlob.get(types.get(fromClazz).getName());
            }
        }

        if (lobProps == null) {
            throw new SQLException("Cannot create blob from " + obj);
        }
        if (obj == null) {
            return null;
        }
        if (obj instanceof Blob) {
            return (Blob)obj;
        }
        if (lobProps.isNullable()) {
            return null;
        }

        Object value = obj;
        Class<?> fromClazz2 = fromClazz;
        if(!areCompatible(fromClazz, obj.getClass())) {
            fromClazz2 = sqlTypeToClass.getOrDefault(md.get().getColumnType(columnIndex), obj.getClass());
        }
        if (fromClazz2 != null && stringBlob.contains(fromClazz2.getName())) {
            value = asString(obj, md, columnIndex);
            fromClazz2 = String.class;
        }
        return new TransportableBlob(toBytes.getOrDefault(fromClazz2, serializableToBytes).apply(value), lobProps.isPartial());
    }

    private boolean areCompatible(Class<?> fromClazz, Class<?> realClazz) {
        if (realClazz == null) {
            return true;
        }
        if (Number.class.isAssignableFrom(fromClazz) && Number.class.isAssignableFrom(realClazz)) {
            return true;
        }
        return fromClazz.equals(realClazz);
    }

    public <T> Clob asClob(T obj, Class<?> fromClazz) throws SQLException {
        return asClob(obj, fromClazz, anyClob);
    }

    public <T> NClob asNClob(T obj, Class<?> fromClazz) throws SQLException {
        return asClob(obj, fromClazz, anyNClob);
    }

    private <T, C extends Clob> C asClob(T obj, Class<?> fromClazz, boolean any) throws SQLException {
        if (!any && !Clob.class.isAssignableFrom(fromClazz)) {
            throw new SQLException("Cannot create clob from " + obj);
        }
        if (obj == null) {
            return null;
        }
        if (Clob.class.isAssignableFrom(obj.getClass())) {
            return (C)obj;
        }
        if (!any) {
            throw new SQLException("Cannot create clob from " + obj);
        }
        return (C)new TransportableClob(obj instanceof Boolean ? toBooleanString((boolean)obj) : obj.toString());
    }

    public <T> Array asArray(T obj) throws SQLException {
        if (obj instanceof Array) {
            return (Array)obj;
        }
        if (anyArray) {
            return obj == null ? null : new TransportableArray(null, 0, new Object[]{obj}, anyArrayPartial);
        }
        throw new SQLException(format("Cannot cast %s to array", obj));
    }

    public <T> Date asDate(T obj, int sqlType) throws SQLException {
        if (obj instanceof Date) {
            return (Date)obj;
        }
        if (obj instanceof Timestamp && toDate.containsKey(Timestamp.class)) {
            Timestamp ts = (Timestamp)obj;
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(ts.getTime());
            Calendar cDate = Calendar.getInstance();
            if (sqlType == Types.TIME_WITH_TIMEZONE || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
                cDate.setTimeZone(timezone);
            }
            if (toDate.get(Timestamp.class).isCopyTimeToTarget()) {
                copyCalendarFields(c, cDate, Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH);
            } else {
                initCalendarFields(cDate, Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND);
            }
            return new Date(cDate.getTimeInMillis());
        }
        if (obj instanceof Time && toDate.containsKey(Time.class)) {
            Time t = (Time)obj;
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(t.getTime());
            Calendar cDate = Calendar.getInstance();
            cDate.setTimeInMillis(0);
            if (toDate.get(Time.class).isCopyTimeToTarget()) {
                copyCalendarFields(c, cDate, Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH);
            } else {
                initCalendarFields(cDate, Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND);
            }
            return new Date(cDate.getTimeInMillis());
        }
        throw new SQLException(format("Cannot cast %s to date", obj));
    }


    private void copyCalendarFields(Calendar src, Calendar dest, int ... fields) {
        for (int field : fields) {
            dest.set(field, src.get(field));
        }
    }

    private void initCalendarFields(Calendar dest, int ... fields) {
        for (int field : fields) {
            dest.set(field, 0);
        }
    }


    public <T> Time asTime(T obj, int sqlType) throws SQLException {
        if (obj instanceof Time) {
            return (Time)obj;
        }
        if (obj instanceof Timestamp && toTime.containsKey(Timestamp.class)) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(((Timestamp)obj).getTime());
            Calendar cTime = Calendar.getInstance();
            if (sqlType == Types.TIME_WITH_TIMEZONE || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
                cTime.setTimeZone(timezone);
            }
            cTime.set(Calendar.YEAR, 1970);
            cTime.set(Calendar.MONTH, Calendar.JANUARY);
            cTime.set(Calendar.DAY_OF_MONTH, 1);
            if (toTime.get(Timestamp.class).isCopyTimeToTarget()) {
                copyCalendarFields(c, cTime, Calendar.YEAR, Calendar.MONTH, Calendar.DAY_OF_MONTH);
            } else {
                initCalendarFields(cTime, Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND);
            }
            return new Time(cTime.getTimeInMillis());
        }
        if (obj instanceof Date && toTime.containsKey(Date.class)) {
            return new Time(0);
        }
        throw new SQLException(format("Cannot cast %s to time", obj));
    }




    public <T> Timestamp asTimestamp(T obj, int sqlType) throws SQLException {
        if (obj instanceof Timestamp) {
            return (Timestamp)obj;
        }
        if (obj instanceof Date && toTimestamp.containsKey(Date.class)) {
            return new Timestamp(((Date)obj).getTime()); }
        if (obj instanceof Time && toTimestamp.containsKey(Time.class)) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(((Time) obj).getTime());
            Calendar cTimestamp = Calendar.getInstance();
            if (sqlType == Types.TIME_WITH_TIMEZONE || sqlType == Types.TIMESTAMP_WITH_TIMEZONE) {
                cTimestamp.setTimeZone(timezone);
            }
            if (!toTimestamp.get(Time.class).isCopyTimeToTarget()) {
                cTimestamp.setTimeInMillis(0);
            }
            copyCalendarFields(c, cTimestamp, Calendar.HOUR_OF_DAY, Calendar.MINUTE, Calendar.SECOND, Calendar.MILLISECOND);
            return new Timestamp(cTimestamp.getTimeInMillis());
        }
        throw new SQLException(format("Cannot cast %s to timestamp", obj));
    }

    public <T> Boolean asBoolean(T obj) throws SQLException {
        if (obj == null) {
            return false;
        }
        Class<?> type = obj.getClass();
        if (toBoolean.contains(type) || toBoolean.contains(types.get(type))) {
            Function<Object, Boolean> castor = toBooleanCastors.get(type);
            if (castor == null && toBoolean.contains(Object.class)) {
                castor = toBooleanCastors.get(Object.class);
            }
            if (castor != null) {
                return castor.apply(obj);
            }
        }
        throw new SQLException(format("Cannot cast %s to boolean", obj));
    }

    private <T> String asFormattedString(T obj) {
        String format = stringFormats.getOrDefault(obj.getClass(), stringFormats.get(types.get(obj.getClass())));
        if (format != null) {
            return String.format(format, obj);
        }
        return obj.toString();
    }

    public String toBooleanString(boolean b) {
        return booleanLiterals.get(b);
    }

    public void throwIfUnsupported(String function) throws SQLFeatureNotSupportedException {
        if (unsupportedFunctions.contains(function)) {
            throw new SQLFeatureNotSupportedException(function);
        }
    }

    public boolean isCharToByte() {
        return charToByte;
    }

    public <T extends Number> ThrowingFunction<Boolean, T, SQLException> booleanToNumber(ThrowingFunction<Boolean, T, SQLException> f) {
        if (booleanToNumber) {
            return f;
        }
        return aBoolean -> {throw new SQLException("Cannot boolean to number");};
    }


    public boolean isBooleanToNumber() {
        return booleanToNumber;
    }

    private static Class toClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(className, e);
        }
    }

    private TimeDataProperties createTimeDataProperties(String s) {
        boolean copyData = false;
        if (s.endsWith("$")) {
            s = s.substring(0, s.length() - 1);
            copyData = true;
        }
        Class clazz = toClass(s);
        return new TimeDataProperties(clazz, copyData);
    }

    private LobProperties createLobProperties(String t) {
        String typeName = t;
        boolean partial = false;
        boolean nullable = false;
        if (t.endsWith(".")) {
            typeName = t.substring(0, t.length() - 1);
            partial = true;
        }
        if (t.endsWith("$")) {
            typeName = t.substring(0, t.length() - 1);
            nullable = true;
        }
        String className = Optional.ofNullable(primitives.get(typeName)).map(Class::getName).orElse(typeName);
        return new LobProperties(typeName, partial, nullable);
    }

    private <C, T> C getPropertyValue(Properties props, String name, Function<String, T> factory, Collector<T, ?, C> collector, C defaultValue) {
        return Optional.ofNullable(props.getProperty(name)).map(p -> Arrays.stream(p.split("\\s*,\\s*"))
                .map(factory)
                .collect(collector)).orElse(defaultValue);
    }

    private Map<Class, TimeDataProperties> getTimePropertiesMap(Properties props, String name) {
        return getPropertyValue(props, name, this::createTimeDataProperties, Collectors.toMap(TimeDataProperties::getClazz, e -> e), emptyMap());
    }
}

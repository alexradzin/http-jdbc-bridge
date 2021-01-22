package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.TimeZone;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
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

    private static final Function<Object, Boolean> numberToBoolean = n -> ((Number)n).longValue() != 0;

    private static Map<Class, Function<Object, Boolean>> toBooleanCastors = Stream.of(
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

    private final boolean timestampWithMillis; //= true;
    private final boolean anyClob;// = true;
    private final boolean anyNClob;// = true;
    private final Collection<String> blobable;// = true;
    private final Collection<String> partialBlobable = new HashSet<>();
    private final Collection<String> nullableBlobable = new HashSet<>();
    private final Round floatToInt;// = Round.INT; // h2 -> floor
    private final Map<Boolean, String> booleanLiterals;
    private final Collection<String> unsupportedFunctions;
    private final Collection<Class> toTimestamp;// = true;
    private final Collection<Class> toBoolean;// = true;

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
        this.blobable = Optional.ofNullable(props.getProperty("blobable")).map(p ->
                Arrays.stream(p.split("\\s*,\\s*"))
                        .map(t -> {
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
                            if (partial) {
                                partialBlobable.add(className);
                            } else if (nullable) {
                                nullableBlobable.add(className);
                            }
                            return className;
                        }).collect(toSet())).orElse(emptySet());
        this.floatToInt = Round.valueOf(props.getProperty("floatToInt", Round.INT.name()));
        String[] boolLiterals = props.getProperty("booleanLiterals", "FALSE,TRUE").split("\\s*,\\s*");
        this.booleanLiterals = new HashMap<>();
        booleanLiterals.put(false, boolLiterals[0]);
        booleanLiterals.put(true, boolLiterals[1]);
        unsupportedFunctions = new HashSet<>(Arrays.asList(props.getProperty("unsupportedFunctions", "").split("\\s*,\\s*")));
        toTimestamp = Optional.ofNullable(props.getProperty("toTimestamp")).map(p -> Arrays.stream(p.split("\\s*,\\s*"))
                        .map(ConnectionProperties::toClass).collect(toList())).orElse(emptyList());
        toBoolean = Stream.concat(
                Stream.of(boolean.class, Boolean.class),
                Optional.ofNullable(props.getProperty("toBoolean")).map(p -> Arrays.stream(p.split("\\s*,\\s*"))
                        .map(typeName -> Optional.ofNullable(primitives.get(typeName)).orElseGet(() -> toClass(typeName)))).orElse(Stream.empty())
        ).collect(Collectors.toList());
    }

    private static boolean getBoolean(Properties props, String name, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(name, "" + defaultValue));
    }

    public long toInteger(double value) {
        return floatToInt.getInt(value);
    }

    private static final DateFormat timestampWithTimezone = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ"); // 2020-10-08 19:18:17+00
    static {
        timestampWithTimezone.setTimeZone(TimeZone.getTimeZone("UTC")); // TODO: probably get it from the current timezone of the DB
    }
    public String asString(Timestamp ts, int type) {
        if (Types.TIMESTAMP_WITH_TIMEZONE == type) {
            return timestampWithTimezone.format(ts).replaceFirst("00$", "");
        }

        String str = ts.toString();
        return timestampWithMillis ? str : str.replaceFirst("\\.\\d+$", "");
    }

    public <T> Blob asBlob(T obj, Class fromClazz) throws SQLException {
        String fromClassName = fromClazz == null ? null : fromClazz.getName();
        if (!(blobable.contains(fromClassName) || (types.containsKey(fromClazz) && blobable.contains(types.get(fromClazz).getName())))) {
            throw new SQLException("Cannot create blob from " + obj);
        }
        if (obj == null) {
            return null;
        }
        if (obj instanceof Blob) {
            return (Blob)obj;
        }
//        boolean nullable = nullableBlobable.contains(fromClassName) || nullableBlobable.contains(types.get(fromClazz).getName());
//        if (nullable) {
//            return null;
//        }

        boolean partial = partialBlobable.contains(fromClassName) || (types.containsKey(fromClazz) && partialBlobable.contains(types.get(fromClazz).getName()));
        return new TransportableBlob(toBytes.getOrDefault(fromClazz, serializableToBytes).apply(obj), partial);
    }

    public <T> Clob asClob(T obj, Class<?> fromClazz) throws SQLException {
        if (!anyClob && !Clob.class.isAssignableFrom(fromClazz)) {
            throw new SQLException("Cannot create clob from " + obj);
        }
        if (obj == null) {
            return null;
        }
        if (obj instanceof Clob) {
            return (Clob)obj;
        }
        if (!anyClob) {
            throw new SQLException("Cannot create clob from " + obj);
        }
        return new TransportableClob(obj instanceof Boolean ? toBooleanString((boolean)obj) : obj.toString());
    }

    public <T> NClob asNClob(T obj, Class<?> fromClazz) throws SQLException {
        if (!anyNClob && !Clob.class.isAssignableFrom(fromClazz)) {
            throw new SQLException("Cannot create nclob from " + obj);
        }
        if (obj == null) {
            return null;
        }
        if (obj instanceof Clob) {
            return (NClob)obj;
        }
        if (!anyNClob) {
            throw new SQLException("Cannot create clob from " + obj);
        }
        return new TransportableClob(obj instanceof Boolean ? toBooleanString((boolean)obj) : obj.toString());
    }

    public <T> Timestamp asTimestamp(T obj, Class<?> fromClass) throws SQLException {
        if (obj instanceof Timestamp) {
            return (Timestamp)obj;
        }
        if (obj instanceof Date && toTimestamp.contains(Date.class)) {
            return new Timestamp(((Date)obj).getTime());
        }
        if (obj instanceof Time && toTimestamp.contains(Time.class)) {
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(((Time)obj).getTime());
            Calendar cTimestamp = Calendar.getInstance();
            cTimestamp.set(Calendar.HOUR_OF_DAY, c.get(Calendar.HOUR_OF_DAY));
            cTimestamp.set(Calendar.MINUTE, c.get(Calendar.MINUTE));
            cTimestamp.set(Calendar.SECOND, c.get(Calendar.SECOND));
            cTimestamp.set(Calendar.MILLISECOND, c.get(Calendar.MILLISECOND));
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

    public String toBooleanString(boolean b) {
        return booleanLiterals.get(b);
    }

    public void throwIfUnsupported(String function) throws SQLFeatureNotSupportedException {
        if (unsupportedFunctions.contains(function)) {
            throw new SQLFeatureNotSupportedException(function);
        }
    }

    private static Class toClass(String className) {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(className, e);
        }
    }
}

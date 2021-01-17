package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ConnectionProperties {
    Map<Class<?>, Function<Object, byte[]>> toBytes =
            Stream.of(
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Boolean.class, o -> ByteBuffer.allocate(1).put((byte)((boolean)o ? 1 : 0)).array()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(String.class, o -> ((String)o).getBytes()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Float.class, i -> ByteBuffer.allocate(4).putFloat(((Number) i).floatValue()).array()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Double.class, i -> ByteBuffer.allocate(8).putDouble(((Number) i).doubleValue()).array()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Character.class, i -> ByteBuffer.allocate(2).putChar(((Character) i)).array()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Byte.class, i -> ByteBuffer.allocate(1).put(((Number) i).byteValue()).array()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Short.class, i -> ByteBuffer.allocate(2).putShort(((Number) i).shortValue()).array()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Integer.class, i -> ByteBuffer.allocate(4).putInt(((Number) i).intValue()).array()),
                    new AbstractMap.SimpleEntry<Class<?>, Function<Object, byte[]>>(Long.class, i -> ByteBuffer.allocate(8).putLong(((Number) i).longValue()).array())
            ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));


    private final boolean timestampWithMillis; //= true;
    private final boolean anyClob;// = true;
    private final boolean anyNClob;// = true;
    private final boolean anyBlob;// = true;
    private final Round floatToInt;// = Round.INT; // h2 -> floor
    private final Map<Boolean, String> booleanLiterals;
    private final Collection<String> unsupportedFunctions;

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
        this.anyBlob = getBoolean(props, "anyBlob", true);
        this.floatToInt = Round.valueOf(props.getProperty("floatToInt", Round.INT.name()));

        String[] boolLiterals = props.getProperty("booleanLiterals", "FALSE,TRUE").split("\\s*,\\s*");
        this.booleanLiterals = new HashMap<>();
        booleanLiterals.put(false, boolLiterals[0]);
        booleanLiterals.put(true, boolLiterals[1]);
        unsupportedFunctions = new HashSet<>(Arrays.asList(props.getProperty("unsupportedFunctions", "").split("\\s*,\\s*")));
    }

    private static boolean getBoolean(Properties props, String name, boolean defaultValue) {
        return Boolean.parseBoolean(props.getProperty(name, "" + defaultValue));
    }

    public long toInteger(double value) {
        return floatToInt.getInt(value);
    }

    public String asString(Timestamp ts) {
        String str = ts.toString();
        return timestampWithMillis ? str : str.replaceFirst("\\.\\d+$", "");
    }

    public <T> Blob asBlob(T obj, Class<?> fromClazz) throws SQLException {
        if (!anyBlob && !Blob.class.isAssignableFrom(fromClazz)) {
            throw new SQLException("Cannot create blob from " + obj);
        }
        if (obj == null) {
            return null;
        }
        if (obj instanceof Blob) {
            return (Blob)obj;
        }
        return new TransportableBlob(toBytes.get(obj.getClass()).apply(obj));
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

    public String toBooleanString(boolean b) {
        return booleanLiterals.get(b);
    }

    public void throwIfUnsupported(String function) throws SQLFeatureNotSupportedException {
        if (unsupportedFunctions.contains(function)) {
            throw new SQLFeatureNotSupportedException(function);
        }
    }

}

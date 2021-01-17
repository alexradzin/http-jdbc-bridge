package com.nosqldriver.jdbc.http.json;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.nosqldriver.jdbc.http.model.TypeMapper;
import com.nosqldriver.util.function.ThrowingTriConsumer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcSerializer<T> extends StdSerializer<T> {
    private final Function<T, Map<String, Object>> getters;
    private static final Map<Class<?>, ThrowingTriConsumer<JsonGenerator, String, Object, IOException>> jgenSetters = Stream.of(
            new SimpleEntry<>(String.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeStringField(name, (String)value)),
            new SimpleEntry<>(Boolean.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeBooleanField(name, (Boolean)value)),
            new SimpleEntry<>(Short.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeNumberField(name, (Short)value)),
            new SimpleEntry<>(Integer.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeNumberField(name, (Integer)value)),
            new SimpleEntry<>(Long.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeNumberField(name, (Long)value)),
            new SimpleEntry<>(BigInteger.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeNumberField(name, (BigInteger)value)),
            new SimpleEntry<>(Float.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeNumberField(name, (Float)value)),
            new SimpleEntry<>(Double.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeNumberField(name, (Double)value)),
            new SimpleEntry<>(BigDecimal.class, (ThrowingTriConsumer<JsonGenerator, String, Object, IOException>) (jgen, name, value) -> jgen.writeNumberField(name, (BigDecimal) value))
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    public JdbcSerializer(Class<T> t, Function<T, Map<String, Object>> getters) {
        super(t);
        this.getters = getters;
    }

    @Override
    public void serialize(T obj, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        jgen.writeStartObject();
        jgen.writeStringField("clazz", obj.getClass().getName());
        Map<String, Object> values = getters.apply(obj);
        for (Entry<String, Object> e : values.entrySet()) {
            String name = e.getKey();
            Object value = e.getValue();
            jgenSetters.get(TypeMapper.get(value.getClass())).accept(jgen, name, value);
        }
        jgen.writeEndObject();
    }
}

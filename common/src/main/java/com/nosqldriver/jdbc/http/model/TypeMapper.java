package com.nosqldriver.jdbc.http.model;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TypeMapper {
    private static final Map<Class<?>, Class<?>> types = Stream.of(
            new SimpleEntry<>(byte.class, Byte.class),
            new SimpleEntry<>(int.class, Integer.class),
            new SimpleEntry<>(long.class, Long.class),
            new SimpleEntry<>(float.class, Float.class),
            new SimpleEntry<>(double.class, Double.class),
            new SimpleEntry<>(boolean.class, Boolean.class)
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    public static Class<?> get(Class<?> type) {
        return types.getOrDefault(type, type);
    }
}

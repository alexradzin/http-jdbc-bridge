package com.nosqldriver.jdbc.http.model;

import com.nosqldriver.util.function.ThrowingFunction;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;

public class ProxyFactory<T> implements ThrowingFunction<Object, T, SQLException> {
    private final Class<? extends T> clazz;
    private final Function<Object, ? extends T> fallback;

    public ProxyFactory(Class<? extends T> clazz) {
        this(clazz, o -> {throw new IllegalArgumentException(String.valueOf(o));});
    }

    public ProxyFactory(Class<? extends T> clazz, Function<Object, ? extends T> fallback) {
        this.clazz = clazz;
        this.fallback = fallback;
    }

    public T create(Map<String, Object> properties) throws ReflectiveOperationException {
        String entityUrl = (String)properties.get("entityUrl");
        T obj = clazz.getConstructor(String.class).newInstance(entityUrl);
        for (Entry<String, Object> e : properties.entrySet().stream().filter(e -> !"entityUrl".equals(e.getKey())).collect(toList())) {
            field(e.getKey()).set(obj, e.getValue());
        }
        return obj;
    }

    private Field field(String name) throws NoSuchFieldException {
        NoSuchFieldException ex = new NoSuchFieldException(name);
        for (Class<?> c = clazz; !Object.class.equals(c); c = c.getSuperclass()) {
            try {
                Field field = c.getDeclaredField(name);
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException e) {
                ex = e;
            }
        }
        throw ex;
    }

    @Override
    public T apply(Object map) {
        try {
            //noinspection unchecked
            return (map instanceof Map) ? create((Map<String, Object>)map) : fallback.apply(map);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

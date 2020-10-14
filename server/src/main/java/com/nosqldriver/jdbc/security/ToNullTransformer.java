package com.nosqldriver.jdbc.security;

public class ToNullTransformer<T> implements DataTransformer<T> {
    @Override
    public T apply(T t) {
        return null;
    }

    @Override
    public Integer get() {
        return 2;
    }
}

package com.nosqldriver.jdbc.security;

public class BypassTransformer<T> implements DataTransformer<T> {
    @Override
    public T apply(T t) {
        return t;
    }

    @Override
    public Integer get() {
        return 3;
    }
}

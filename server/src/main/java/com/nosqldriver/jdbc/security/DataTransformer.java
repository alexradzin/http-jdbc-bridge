package com.nosqldriver.jdbc.security;

import java.util.function.Function;
import java.util.function.Supplier;

public interface DataTransformer<T> extends Function<T, T>, Supplier<Integer> {
    @Override Integer get();
    @Override T apply(T t);
}

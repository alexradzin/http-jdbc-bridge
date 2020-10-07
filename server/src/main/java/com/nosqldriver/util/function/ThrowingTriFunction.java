package com.nosqldriver.util.function;

@FunctionalInterface
public interface ThrowingTriFunction<T, U, V, R, E extends Throwable> {
    R apply(T t, U u, V v) throws E;
}

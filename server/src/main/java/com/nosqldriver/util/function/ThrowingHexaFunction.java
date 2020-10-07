package com.nosqldriver.util.function;

@FunctionalInterface
public interface ThrowingHexaFunction<T, U, V, W, X, Y, R, E extends Throwable> {
    R apply(T t, U u, V v, W w, Y y, X x) throws E;
}

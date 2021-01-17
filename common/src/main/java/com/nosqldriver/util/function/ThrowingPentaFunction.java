package com.nosqldriver.util.function;

@FunctionalInterface
public interface ThrowingPentaFunction<T, U, V, W, X, R, E extends Throwable> {
    R apply(T t, U u, V v, W w, X x) throws E;
}

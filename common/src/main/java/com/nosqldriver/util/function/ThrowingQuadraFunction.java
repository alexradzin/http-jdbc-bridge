package com.nosqldriver.util.function;

@FunctionalInterface
public interface ThrowingQuadraFunction<T, U, V, W, R, E extends Throwable> {
    R apply(T t, U u, V v, W w) throws E;
}

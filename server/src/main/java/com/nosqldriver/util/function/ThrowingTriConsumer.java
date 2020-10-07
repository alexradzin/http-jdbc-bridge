package com.nosqldriver.util.function;

@FunctionalInterface
public interface ThrowingTriConsumer<T, U, V, E extends Throwable> {
    void accept(T t, U u, V v) throws E;
}

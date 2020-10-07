package com.nosqldriver.util.function;

@FunctionalInterface
public interface ThrowingSeptaFunction<T, U, V, W, X, Y, Z, R, E extends Throwable> {
    R apply(T t, U u, V v, W w, Y y, X x, Z z) throws E;
}

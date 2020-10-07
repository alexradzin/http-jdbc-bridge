package com.nosqldriver.jdbc.http;

public class SneakyThrower {
    @SuppressWarnings("unchecked")
    public static <R, E extends Throwable> R sneakyThrow(Throwable e) throws E {
        throw (E) e;
    }
}

package com.nosqldriver.jdbc.security;

import java.security.AccessControlException;

public class AccessDenied<T> implements DataTransformer<T> {

    @Override
    public T apply(T t) {
        throw new AccessControlException("Access to this resource is not allowed");
    }

    @Override
    public Integer get() {
        return 0;
    }
}

package com.nosqldriver.jdbc.http.model;

import java.sql.Wrapper;

import static com.nosqldriver.jdbc.http.Util.pathParameter;
import static java.lang.String.format;

abstract class WrapperProxy extends EntityProxy implements Wrapper {
    protected WrapperProxy(String entityUrl, Class<?> clazz) {
        super(entityUrl, clazz);
    }

    @Override
    public final <T> T unwrap(Class<T> iface) {
        //noinspection unchecked
        return (T)connector.get(format("%s/unwrap%s", entityUrl, pathParameter(iface)), Object.class);
    }

    @Override
    public final boolean isWrapperFor(Class<?> iface) {
        return connector.get(format("%s/wrapper%s", entityUrl, pathParameter(iface)), Boolean.class);
    }
}

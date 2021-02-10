package com.nosqldriver.jdbc.http.model;

class LobProperties {
    private final String className;
    private final boolean partial;
    private final boolean nullable;

    LobProperties(String className, boolean partial, boolean nullable) {
        this.className = className;
        this.partial = partial;
        this.nullable = nullable;
    }

    String getClassName() {
        return className;
    }

    boolean isPartial() {
        return partial;
    }

    boolean isNullable() {
        return nullable;
    }
}

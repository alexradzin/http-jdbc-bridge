package com.nosqldriver.jdbc.http.model;

class TimeDataProperties {
    private final Class clazz;
    private final boolean copyTimeToTarget;

    TimeDataProperties(Class clazz, boolean copyTimeToTarget) {
        this.clazz = clazz;
        this.copyTimeToTarget = copyTimeToTarget;
    }

    Class getClazz() {
        return clazz;
    }

    boolean isCopyTimeToTarget() {
        return copyTimeToTarget;
    }
}

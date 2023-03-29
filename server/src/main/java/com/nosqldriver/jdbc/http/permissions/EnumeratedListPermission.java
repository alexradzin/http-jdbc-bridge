package com.nosqldriver.jdbc.http.permissions;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.lang.String.format;

public class EnumeratedListPermission<T> implements Function<T, SQLException> {
    private final Function<T, List<String>> getter;
    private final String forbiddenMessage;
    private final List<String> permitted;

    public EnumeratedListPermission(Function<T, List<String>> getter, String forbiddenMessage, List<String> permitted) {
        this.getter = getter;
        this.forbiddenMessage = forbiddenMessage;
        this.permitted = permitted;
    }

    public List<String> getPermitted() {
        return permitted;
    }

    @Override
    public SQLException apply(T query) {
        List<String> actual = getter.apply(query);
        if (permitted.contains("*") || actual.isEmpty()) {
            return null;
        }
        if(!permitted.containsAll(actual)) {
            List<String> actualCopy = new ArrayList<>(actual);
            actualCopy.removeAll(permitted);
            return new SQLException(format(forbiddenMessage, actualCopy)); // "Query groups by forbidden field"
        }
        return null;
    }
}

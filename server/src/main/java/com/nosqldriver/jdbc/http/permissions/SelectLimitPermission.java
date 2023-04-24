package com.nosqldriver.jdbc.http.permissions;

import java.sql.SQLException;
import java.util.Optional;
import java.util.function.Function;

import static java.lang.String.format;

public class SelectLimitPermission<T> implements Function<T, SQLException> {
    private final Function<T, Optional<Long>> limitGetter;
    private final int limit;

    public SelectLimitPermission(Function<T, Optional<Long>> limitGetter, int limit) {
        this.limitGetter = limitGetter;
        this.limit = limit;
    }

    @Override
    public SQLException apply(T query) {
        Optional<Long> optLimit = limitGetter.apply(query);
        if (optLimit.isPresent()) {
            long limit = optLimit.get();
            if (limit > this.limit) {
                return new SQLException(format("Actual limit %d exceeds required one %d", limit, this.limit));
            }
        } else {
            return new SQLException("Query must be limited but was not");
        }
        return null;
    }
}

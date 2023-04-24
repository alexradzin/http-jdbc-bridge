package com.nosqldriver.jdbc.http.permissions;

import java.sql.SQLException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class WherePermission<T> implements Function<T, SQLException> {
    private final Function<T, List<Entry<String, ComparisonOperation>>> getter;
    private final List<ComparisonPermission> comparisonPermissions;

    public WherePermission(Function<T, List<Entry<String, ComparisonOperation>>> getter, List<ComparisonPermission> comparisonPermissions) {
        this.getter = getter;
        this.comparisonPermissions = comparisonPermissions;
    }

    @Override
    public SQLException apply(T statement) {
        Optional<List<Entry<String, ComparisonOperation>>> operations = Optional.ofNullable(getter.apply(statement));
        boolean noWhereClauses = operations.map(List::isEmpty).orElse(true);
        return operations
                .flatMap(where -> where.stream().flatMap(expression -> comparisonPermissions.stream().map(permission -> permission.apply(expression.getKey(), expression.getValue())))
                .filter(Objects::nonNull)
                .findFirst())
                .orElse(noWhereClauses && !comparisonPermissions.isEmpty() ? new SQLException("where clause is required here") : null);
    }
}

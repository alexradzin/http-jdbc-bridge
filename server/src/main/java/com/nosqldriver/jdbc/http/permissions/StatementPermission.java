package com.nosqldriver.jdbc.http.permissions;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

public class StatementPermission<T> implements Predicate<String>, Comparable<Predicate<String>> {
    private final Map<String, Function<T, SQLException>> elementPermissions;
    private final List<String> targets;
    private final Function<T, String> tableNameGetter;

    public StatementPermission(Map<String, Function<T, SQLException>> elementPermissions, List<String> targets, Function<T, String> tableNameGetter) {
        this.elementPermissions = elementPermissions;
        this.targets  = targets;
        this.tableNameGetter = tableNameGetter;
    }

    @Override
    public int compareTo(Predicate<String> other) {
        boolean thisAll = isAll(targets);
        if (other instanceof StatementPermission) {
            List<String> otherTargets = ((StatementPermission<?>)other).targets;
            boolean otherAll = isAll(otherTargets);
            if (thisAll) {
                return otherAll ? 0 : 1;
            }
            if (otherAll) {
                return -1;
            }
            return targets.size() - otherTargets.size();
        }

        return thisAll ? 1 : -1;
    }

    private boolean isAll(List<String> targets) {
        return targets.contains("*");
    }

    @Override
    public boolean test(String s) {
        return isAll(targets) || targets.contains(s);
    }

    public boolean validate(T statement, Map<String, Boolean> doneFlags) throws SQLException {
        if (!test(tableNameGetter.apply(statement))) {
            return false;
        }
        AtomicBoolean validated = new AtomicBoolean(false);
        Optional<SQLException> error = elementPermissions.entrySet().stream()
                .filter(e -> doneFlags.containsKey(e.getKey()))
                .filter(e -> !doneFlags.get(e.getKey()))
                .peek(e -> doneFlags.put(e.getKey(), true))
                .peek(e -> validated.set(true))
                .map(e -> e.getValue().apply(statement))
                .filter(Objects::nonNull)
                .findFirst();

        if (error.isPresent()) {
            throw error.get();
        }
        return elementPermissions.isEmpty() || validated.get();
    }
}

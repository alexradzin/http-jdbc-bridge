package com.nosqldriver.jdbc.http.permissions;

import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

public class StatementPermissions<T> {
    private final Collection<StatementPermission<T>> permissions;
    private final Collection<String> elementNames;

    public StatementPermissions(Collection<StatementPermission<T>> permissions, Collection<String> elementNames) {
        this.permissions = permissions;
        this.elementNames = elementNames;
    }

    public boolean validate(T statement) throws SQLException {
        Map<String, Boolean> doneFlags = new HashMap<>(elementNames.stream().collect(toMap(element -> element, element -> false)));
        boolean validated = false;
        for (StatementPermission<T> permission : permissions) {
            if (permission.validate(statement, doneFlags)) {
                validated = true;
            }
        }
        if (!validated) {
            throw new SQLException("Statement is not allowed");
        }
        return validated;
    }
}

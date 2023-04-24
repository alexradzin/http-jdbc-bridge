package com.nosqldriver.jdbc.http.permissions;

import java.util.Map;
import java.util.function.Predicate;

public class UserPermissions {
    private final Predicate<String> jdbcUrlPermission;
    private final Map<String, Predicate<String>> queryPermission;

    public UserPermissions(Predicate<String> jdbcUrlPermission, Map<String, Predicate<String>> queryPermission) {
        this.jdbcUrlPermission = jdbcUrlPermission;
        this.queryPermission = queryPermission;
    }

    public boolean isUrlAllowed(String url) {
        return jdbcUrlPermission.test(url);
    }

    public boolean isQueryAllowed(String query) {
        return false;
    }
}

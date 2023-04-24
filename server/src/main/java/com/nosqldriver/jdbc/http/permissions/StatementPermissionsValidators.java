package com.nosqldriver.jdbc.http.permissions;

import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingFunction;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class StatementPermissionsValidators implements ThrowingBiFunction<String, String, String, SQLException> {
    private final Map<String, ThrowingFunction<String, String, SQLException>> permissionValidators = new HashMap<>();
    private volatile ThrowingFunction<String, String, SQLException> defaultPermissionValidator;

    public StatementPermissionsValidators addConfiguration(String userName, ThrowingFunction<String, String, SQLException> permissions) {
        permissionValidators.put(userName, permissions);
        return this;
    }

    public StatementPermissionsValidators removeConfiguration(String userName) {
        permissionValidators.remove(userName);
        return this;
    }

    public StatementPermissionsValidators setDefaultConfiguration(ThrowingFunction<String, String, SQLException> permissions) {
        defaultPermissionValidator = permissions;
        return this;
    }

    @Override
    public String apply(String user, String query) throws SQLException {
        return permissionValidators.getOrDefault(user, defaultPermissionValidator).apply(query);
    }
}

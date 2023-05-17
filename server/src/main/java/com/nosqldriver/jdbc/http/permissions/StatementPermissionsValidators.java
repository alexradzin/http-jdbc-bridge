package com.nosqldriver.jdbc.http.permissions;

import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingFunction;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class StatementPermissionsValidators implements ThrowingBiFunction<String, String, String, SQLException> {
    private static final ThrowingFunction<String, String, SQLException> allowAll = query -> query;
    private final Map<String, ThrowingFunction<String, String, SQLException>> permissionValidators = new HashMap<>();

    public StatementPermissionsValidators addConfiguration(String userName, ThrowingFunction<String, String, SQLException> permissions) {
        permissionValidators.put(userName, permissions);
        return this;
    }

    public StatementPermissionsValidators removeConfiguration(String userName) {
        permissionValidators.remove(userName);
        permissionValidators.entrySet().removeIf(entry -> entry.getKey().startsWith(userName + "/"));
        return this;
    }

    @Override
    public String apply(String user, String query) throws SQLException {
        if (user != null) {
            for (String key = user; !"".equals(key); key = key.replaceFirst("/?[^/]+$", "")) {
                ThrowingFunction<String, String, SQLException> f = permissionValidators.get(key);
                if (f != null) {
                    return f.apply(query);
                }
            }
        }
        return permissionValidators.getOrDefault("", allowAll).apply(query);
    }
}

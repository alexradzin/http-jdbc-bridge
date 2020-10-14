package com.nosqldriver.jdbc.security;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.Comparator.comparingInt;

public class PermissionsManager {
    private Map<String, Collection<String>> userRoles = null;
    private Map<PermissionKey, DataTransformer> transformers = new HashMap<>();

    public <T> Function<T, T> transformer(String user, String resource) {
        return Optional.ofNullable(transformers(user, role -> new PermissionKey(role, resource)).min(comparingInt(DataTransformer::get))
                .orElseGet(() -> transformers(user, role -> new PermissionKey(role, null)).min(comparingInt(DataTransformer::get))
                        .orElseGet(() -> transformers.getOrDefault(new PermissionKey(null, resource), transformers.get(new PermissionKey(null, null)))))).orElse(new BypassTransformer<T>());
    }

    private Stream<DataTransformer> transformers(String user, Function<String, PermissionKey> pkFactory) {
        return userRoles.getOrDefault(user, Collections.emptyList()).stream()
                .map(role -> transformers.get(pkFactory.apply(role)))
                .filter(Objects::nonNull);
    }

    public static class PermissionKey {
        private final String role;
        private final String resource;

        public PermissionKey(String role, String resource) {
            this.role = role;
            this.resource = resource;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PermissionKey that = (PermissionKey) o;
            return role.equals(that.role) && resource.equals(that.resource);
        }

        @Override
        public int hashCode() {
            return Objects.hash(role, resource);
        }
    }
}

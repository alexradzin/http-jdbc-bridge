package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

public class JdbcUrlsProvider implements ArgumentsProvider {
    private static final String JDBC_PROP = "jdbc.urls";
    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        return Arrays.stream(Optional.ofNullable(System.getProperty(JDBC_PROP, System.getenv(JDBC_PROP))).map(p -> p.split(",")).orElse(new String[]{
                "jdbc:h2:mem:test",
                "jdbc:hsqldb:mem",
                "jdbc:derby:memory:test;create=true",
                "jdbc:mysql://127.0.0.1:3306/test?user=root",
                "jdbc:postgresql://localhost:5432/test?user=postgres",
        })).map(Arguments::of);
    }
}

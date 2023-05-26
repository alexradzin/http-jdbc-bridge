package com.nosqldriver.util.function;

import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Configuration {
    public static String getConfigurationParameter(String propertyName, String defaultValue) {
        Stream<Supplier<String>> suppliers = Stream.of(
                () -> System.getProperty(propertyName, System.getenv(propertyName)),
                () -> System.getenv(propertyName.toUpperCase().replace('.', '_')),
                () -> defaultValue);
        return suppliers.map(Supplier::get).filter(Objects::nonNull).findFirst().orElse(null);
    }
}

package com.nosqldriver.util.function;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class ConfigurationTest {
    @Test
    void prop() {
        System.out.println(System.getProperties());
        assertEquals(System.getProperty("java.specification.version"), Configuration.getConfigurationParameter("java.specification.version", null));
    }

    @Test
    void env() {
        List<String> envKeys = new ArrayList<>(System.getenv().keySet());
        List<String> propKeys = System.getProperties().keySet().stream().map(k -> (String)k).collect(Collectors.toList());
        envKeys.removeAll(propKeys);
        envKeys.forEach(key -> assertEquals(System.getenv(key), Configuration.getConfigurationParameter(key.toLowerCase().replace('_', '.'), null)));
    }
}

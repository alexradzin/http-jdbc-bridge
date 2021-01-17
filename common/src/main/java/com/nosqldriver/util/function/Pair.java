package com.nosqldriver.util.function;

import java.util.AbstractMap;
import java.util.Map;

public class Pair {
    public static <K, V> Map.Entry<K, V> pair(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}

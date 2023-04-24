package com.nosqldriver.jdbc.http.permissions;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

public enum ComparisonOperation {
    EQ("="), NE("<>"), GE(">="), GT(">"), LE("<="), LT("<"), IN("in"), LIKE("like"), ANY("*"),;

    private final String op;
    private final static Map<String, ComparisonOperation> byOp = Stream.of(ComparisonOperation.values()).collect(toMap(e -> e.op, e ->  e));

    ComparisonOperation(String op) {
        this.op = op;
    }

    public String getOperator() {
        return op;
    }

    public static ComparisonOperation valueByOperation(String op) {
        return Optional.ofNullable(byOp.get(op))
                .orElseThrow(() -> new IllegalArgumentException(format("No enum operation %s.%s", ComparisonOperation.class.getName(), op)));
    }
}

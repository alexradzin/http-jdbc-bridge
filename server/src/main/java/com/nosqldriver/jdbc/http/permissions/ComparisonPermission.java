package com.nosqldriver.jdbc.http.permissions;

import java.sql.SQLException;
import java.util.List;
import java.util.function.BiFunction;

import static java.lang.String.format;

public class ComparisonPermission implements BiFunction<String, ComparisonOperation, SQLException> {
    private final List<String> permittedFields;
    private final List<ComparisonOperation> permittedComparisonOperations;

    public ComparisonPermission(String permittedField, ComparisonOperation permittedComparisonOperation) {
        this(permittedField, List.of(permittedComparisonOperation));
    }

    public ComparisonPermission(String permittedField, List<ComparisonOperation> permittedComparisonOperations) {
        this(List.of(permittedField), permittedComparisonOperations);
    }

    public ComparisonPermission(List<String> permittedFields, List<ComparisonOperation> permittedComparisonOperations) {
        this.permittedFields = permittedFields;
        this.permittedComparisonOperations = permittedComparisonOperations;
    }

    @Override
    public SQLException apply(String field, ComparisonOperation comparison) {
        boolean anyField = permittedFields.contains("*");
        boolean anyOp = permittedComparisonOperations.contains(ComparisonOperation.ANY);
        if ((!permittedFields.contains(field) && !anyField) || (!permittedComparisonOperations.contains(comparison) && !anyOp)) {
            return new SQLException(format("Condition %s %s is forbidden", field, comparison.getOperator()));
        }
        return null;
    }
}

package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AssertUtils {
    private static final Collection<Class<?>> integerTypes = new HashSet<>(Arrays.asList(Byte.class, Short.class, Integer.class, Long.class));
    private static final Collection<Class<?>> floatingTypes = new HashSet<>(Arrays.asList(Float.class, Double.class));

    public static void assertResultSet(ResultSet expected, ResultSet actual, String message) throws SQLException {
        if (expected == null) {
            assertNull(actual);
            return;
        }
        assertResultSetMetaData(expected.getMetaData(), actual.getMetaData(), message);
        ResultSetMetaData md = expected.getMetaData();
        int n = md.getColumnCount();

        while (expected.next() && actual.next()) {
            for (int i = 1; i <= n; i++) {
                assertValues(expected.getObject(i), actual.getObject(i), format("%s:column#%d:%s", message, i, md.getColumnName(i)));
            }
        }

        assertFalse(expected.next(), format("%s:expected result set has extra rows", message));
        assertFalse(actual.next(), format("%s:actual result set has extra rows", message));
    }

    public static void assertResultSetMetaData(ResultSetMetaData expected, ResultSetMetaData actual, String message) throws SQLException {
        assertEquals(expected.getColumnCount(), actual.getColumnCount());
        int n = expected.getColumnCount();
        String fmt = "%s:%s#%d";
        for (int i = 1; i <= n; i++) {
            assertEquals(expected.getColumnName(i), actual.getColumnName(i), format(fmt, message, "ColumnName", i));
            assertEquals(expected.getColumnType(i), actual.getColumnType(i), format(fmt, message, "ColumnType", i));
            assertEquals(expected.getColumnTypeName(i), actual.getColumnTypeName(i), format(fmt, message, "ColumnTypeName", i));
            assertEquals(expected.getColumnClassName(i), actual.getColumnClassName(i), format(fmt, message, "ColumnClassName", i));
            assertEquals(expected.getColumnLabel(i), actual.getColumnLabel(i), format(fmt, message, "ColumnLabel", i));
            assertEquals(expected.getColumnDisplaySize(i), actual.getColumnDisplaySize(i), format(fmt, message, "ColumnDisplaySize", i));
            assertEquals(expected.getPrecision(i), actual.getPrecision(i), format(fmt, message, "Precision", i));
            assertEquals(expected.getScale(i), actual.getScale(i), format(fmt, message, "Scale", i));
            assertEquals(expected.getCatalogName(i), actual.getCatalogName(i), format(fmt, message, "CatalogName", i));
            assertEquals(expected.getSchemaName(i), actual.getSchemaName(i), format(fmt, message, "SchemaName", i));
            assertEquals(expected.getTableName(i), actual.getTableName(i), format(fmt, message, "TableName", i));
            assertEquals(expected.isAutoIncrement(i), actual.isAutoIncrement(i), format(fmt, message, "AutoIncrement", i));
            assertEquals(expected.isCaseSensitive(i), actual.isCaseSensitive(i), format(fmt, message, "CaseSensitive", i));
            assertEquals(expected.isCurrency(i), actual.isCurrency(i), format(fmt, message, "Currency", i));
            assertEquals(expected.isDefinitelyWritable(i), actual.isDefinitelyWritable(i), format(fmt, message, "DefinitelyWritable", i));
            assertEquals(expected.isNullable(i), actual.isNullable(i), format(fmt, message, "Nullable", i));
            assertEquals(expected.isReadOnly(i), actual.isReadOnly(i), format(fmt, message, "ReadOnly", i));
            assertEquals(expected.isSearchable(i), actual.isSearchable(i), format(fmt, message, "Searchable", i));
            assertEquals(expected.isSigned(i), actual.isSigned(i), format(fmt, message, "Signed", i));
            assertEquals(expected.isWritable(i), actual.isWritable(i), format(fmt, message, "Writable", i));
        }
    }

    public static void assertValues(Object expected, Object actual, String message) {
        if (isInteger(expected) && isInteger(actual)) {
            assertEquals(((Number)expected).longValue(), ((Number)actual).longValue(), message);
        } else if (isFloating(expected) && isFloating(actual)) {
            assertEquals(((Number)expected).doubleValue(), ((Number)actual).doubleValue(), message);
        } else {
            assertEquals(expected, actual, message);
        }
    }

    public static boolean isInteger(Object obj) {
        return obj != null && integerTypes.contains(obj.getClass());
    }

    public static boolean isFloating(Object obj) {
        return obj != null && floatingTypes.contains(obj.getClass());
    }

    public static <T> void assertCall(ThrowingFunction<T, ?, SQLException> f, T nativeObj, T httpObj, String message) {
        Object nativeRes = null;
        Object httpRes = null;
        SQLException nativeEx = null;
        SQLException httpEx = null;

        try {
            nativeRes = f.apply(nativeObj);
        } catch (SQLException e) {
            nativeEx = e;
        }
        try {
            httpRes = f.apply(httpObj);
        } catch (SQLException e) {
            httpEx = e;
        }
        if (nativeEx == null) {
            if (nativeRes == null) {
                assertNull(httpRes, message);
            } else {
                assertNotNull(httpRes, message);
            }
        } else {
            assertNotNull(httpEx, message);
            assertEquals(nativeEx.getMessage(), httpEx.getMessage(), message);
        }
    }

    public static <T> void assertCall(ThrowingConsumer<T, SQLException> f, T nativeObj, T httpObj, String message) {
        SQLException nativeEx = null;
        SQLException httpEx = null;

        try {
            f.accept(nativeObj);
        } catch (SQLException e) {
            nativeEx = e;
        }
        try {
            f.accept(httpObj);
        } catch (SQLException e) {
            httpEx = e;
        }
        if (nativeEx == null) {
            assertNull(httpEx, message);
        } else {
            assertNotNull(httpEx, message);
        }
    }

    public static <T> void assertGettersAndSetters(Collection<Map.Entry<String, Map.Entry<ThrowingFunction<T, ?, SQLException>, ThrowingConsumer<T, SQLException>>>> functions, T nativeObj, T httpObj) {
        for (Map.Entry<String, Map.Entry<ThrowingFunction<T, ?, SQLException>, ThrowingConsumer<T, SQLException>>> function : functions) {
            String name = function.getKey();
            ThrowingFunction<T, ?, SQLException> getter = function.getValue().getKey();
            ThrowingConsumer<T, SQLException> setter = function.getValue().getValue();
            assertCall(getter, nativeObj, httpObj, name); // first call getter
            assertCall(setter, nativeObj, httpObj, name); // now call corresponding setter
            assertCall(getter, nativeObj, httpObj, name); // call getter again to be sure that setter worked
        }
    }
}

package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingBiConsumer;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AssertUtils {
    private static final Collection<Class<?>> integerTypes = new HashSet<>(asList(Byte.class, Short.class, Integer.class, Long.class));
    private static final Collection<Class<?>> floatingTypes = new HashSet<>(asList(Float.class, Double.class, BigDecimal.class));
    private static final Map<Integer, Collection<ThrowingBiFunction<ResultSet, Integer, ?, SQLException>>> typedGettersByIndex = new HashMap<>();
    static {
        typedGettersByIndex.put(Types.BIT, asList(ResultSet::getBoolean, ResultSet::getObject));
        typedGettersByIndex.put(Types.TINYINT, asList(ResultSet::getByte, ResultSet::getShort, ResultSet::getInt, ResultSet::getLong));
        typedGettersByIndex.put(Types.SMALLINT, asList(ResultSet::getShort, ResultSet::getInt, ResultSet::getLong));
        typedGettersByIndex.put(Types.INTEGER, asList(ResultSet::getInt, ResultSet::getLong));
        typedGettersByIndex.put(Types.BIGINT, asList(ResultSet::getLong));
        typedGettersByIndex.put(Types.FLOAT, asList(ResultSet::getFloat, ResultSet::getDouble));
        typedGettersByIndex.put(Types.REAL, asList(ResultSet::getDouble));
        typedGettersByIndex.put(Types.DOUBLE, asList(ResultSet::getDouble));
        typedGettersByIndex.put(Types.NUMERIC, asList(ResultSet::getDouble));
        typedGettersByIndex.put(Types.DECIMAL, asList(ResultSet::getBigDecimal));
        typedGettersByIndex.put(Types.CHAR, asList((rs, i) -> {String s = rs.getString(i); return s == null ? null : s.length() > 0 ? s.substring(0, 1) : "";}));
        typedGettersByIndex.put(Types.VARCHAR, asList(ResultSet::getString));
        typedGettersByIndex.put(Types.LONGVARCHAR, asList(ResultSet::getString));
        typedGettersByIndex.put(Types.DATE, asList(ResultSet::getDate));
        typedGettersByIndex.put(Types.TIME, asList(ResultSet::getTime));
        typedGettersByIndex.put(Types.TIME_WITH_TIMEZONE, asList(ResultSet::getTime));
        typedGettersByIndex.put(Types.TIMESTAMP, asList(ResultSet::getTimestamp));
        typedGettersByIndex.put(Types.TIMESTAMP_WITH_TIMEZONE, asList(ResultSet::getTimestamp));
        //getters.put(Types.BINARY, ResultSet::getObject);
        //getters.put(Types.VARBINARY, ResultSet::getInt);
        //getters.put(Types.LONGVARBINARY, ResultSet::getInt);
        typedGettersByIndex.put(Types.NULL, asList(ResultSet::getObject));
        //getters.put(Types.OTHER, ResultSet::getObject);
        //getters.put(Types.JAVA_OBJECT, ResultSet::getObject);
        //getters.put(Types.DISTINCT, ResultSet::getInt);
        //getters.put(Types.STRUCT, ResultSet::getInt);
        typedGettersByIndex.put(Types.ARRAY, asList(ResultSet::getArray));
        typedGettersByIndex.put(Types.BLOB, asList(ResultSet::getBlob));
        typedGettersByIndex.put(Types.CLOB, asList(ResultSet::getClob));
        typedGettersByIndex.put(Types.REF, asList(ResultSet::getRef));
        //getters.put(Types.DATALINK, ResultSet::getObject);
        typedGettersByIndex.put(Types.BOOLEAN, asList(ResultSet::getBoolean));
        typedGettersByIndex.put(Types.ROWID, asList(ResultSet::getRowId));
    }

    private static final Map<Integer, Collection<ThrowingBiFunction<ResultSet, String, ?, SQLException>>> typedGettersByLabel = new HashMap<>();
    static {
        typedGettersByLabel.put(Types.BIT, asList(ResultSet::getBoolean, ResultSet::getObject));
        typedGettersByLabel.put(Types.TINYINT, asList(ResultSet::getByte, ResultSet::getShort, ResultSet::getInt, ResultSet::getLong));
        typedGettersByLabel.put(Types.SMALLINT, asList(ResultSet::getShort, ResultSet::getInt, ResultSet::getLong));
        typedGettersByLabel.put(Types.INTEGER, asList(ResultSet::getInt, ResultSet::getLong));
        typedGettersByLabel.put(Types.BIGINT, asList(ResultSet::getLong));
        typedGettersByLabel.put(Types.FLOAT, asList(ResultSet::getFloat, ResultSet::getDouble));
        typedGettersByLabel.put(Types.REAL, asList(ResultSet::getDouble));
        typedGettersByLabel.put(Types.DOUBLE, asList(ResultSet::getDouble));
        typedGettersByLabel.put(Types.NUMERIC, asList(ResultSet::getDouble));
        typedGettersByLabel.put(Types.DECIMAL, asList(ResultSet::getBigDecimal));
        typedGettersByLabel.put(Types.CHAR, asList((rs, i) -> {String s = rs.getString(i); return s == null ? null : s.length() > 0 ? s.substring(0, 1) : "";}));
        typedGettersByLabel.put(Types.VARCHAR, asList(ResultSet::getString));
        typedGettersByLabel.put(Types.LONGVARCHAR, asList(ResultSet::getString));
        typedGettersByLabel.put(Types.DATE, asList(ResultSet::getDate));
        typedGettersByLabel.put(Types.TIME, asList(ResultSet::getTime));
        typedGettersByLabel.put(Types.TIME_WITH_TIMEZONE, asList(ResultSet::getTime));
        typedGettersByLabel.put(Types.TIMESTAMP, asList(ResultSet::getTimestamp));
        typedGettersByLabel.put(Types.TIMESTAMP_WITH_TIMEZONE, asList(ResultSet::getTimestamp));
        //getters.put(Types.BINARY, ResultSet::getObject);
        //getters.put(Types.VARBINARY, ResultSet::getInt);
        //getters.put(Types.LONGVARBINARY, ResultSet::getInt);
        typedGettersByLabel.put(Types.NULL, asList(ResultSet::getObject));
        //getters.put(Types.OTHER, ResultSet::getObject);
        //getters.put(Types.JAVA_OBJECT, ResultSet::getObject);
        //getters.put(Types.DISTINCT, ResultSet::getInt);
        //getters.put(Types.STRUCT, ResultSet::getInt);
        typedGettersByLabel.put(Types.ARRAY, asList(ResultSet::getArray));
        typedGettersByLabel.put(Types.BLOB, asList(ResultSet::getBlob));
        typedGettersByLabel.put(Types.CLOB, asList(ResultSet::getClob));
        typedGettersByLabel.put(Types.REF, asList(ResultSet::getRef));
        //getters.put(Types.DATALINK, ResultSet::getObject);
        typedGettersByLabel.put(Types.BOOLEAN, asList(ResultSet::getBoolean));
        typedGettersByLabel.put(Types.ROWID, asList(ResultSet::getRowId));
    }


    /**
     *
     * @param nativeUrl
     * @param expected
     * @param actual
     * @param message
     * @param limit
     * @param advanced - exists for performance reasons
     * @return
     * @throws SQLException
     */
    public static Collection<Map<String, Object>> assertResultSet(String nativeUrl, ResultSet expected, ResultSet actual, String message, int limit, boolean advanced) throws SQLException {
        if (expected == null) {
            assertNull(actual);
            return null;
        }
        Collection<Map<String, Object>> result = new ArrayList<>();
        assertResultSetMetaData(expected.getMetaData(), actual.getMetaData(), message);
        ResultSetMetaData emd = expected.getMetaData();
        ResultSetMetaData amd = actual.getMetaData();
        int n = emd.getColumnCount();
        assertEquals(emd.getColumnCount(), amd.getColumnCount());

        assertEquals(expected.getFetchDirection(), actual.getFetchDirection());
        assertEquals(expected.getType(), actual.getType());
        assertEquals(expected.getConcurrency(), actual.getConcurrency());

        int row = 0;
        boolean checkExtraRows = true;

        assertResultSetNavigationState(nativeUrl, expected, actual);
        while (next(expected, actual)) {
            assertResultSetNavigationState(nativeUrl, expected, actual);
            Map<String, Object> rowData = new LinkedHashMap<>();
            for (int i = 1; i <= n; i++) {
                for (ThrowingBiFunction<ResultSet, Integer, ?, SQLException> getter : typedGettersByIndex.getOrDefault(emd.getColumnType(i), Collections.singleton(ResultSet::getObject))) {
                    assertValues(getter.apply(expected, i), getter.apply(actual, i), format("%s:column#%d:%s", message, i, emd.getColumnName(i)));
                }
                for (ThrowingBiFunction<ResultSet, String, ?, SQLException> getter : typedGettersByLabel.getOrDefault(emd.getColumnType(i), Collections.singleton(ResultSet::getObject))) {
                    int type = emd.getColumnType(i);
                    if (nativeUrl.startsWith("jdbc:derby") && (type == Types.BLOB || type == Types.CLOB)) {
                        continue; // patch for derby that does not allow to retrieve blobs and clobs more than once. The first time they were retrieved by index.
                    }
                    String label = emd.getColumnLabel(i);
                    assertEquals(expected.findColumn(label), actual.findColumn(label));
                    Object actualValue = getter.apply(actual, label);
                    assertValues(getter.apply(expected, emd.getColumnLabel(i)), actualValue, format("%s:column#%s", message, emd.getColumnLabel(i)));
                    rowData.put(label, actualValue);

                    if (advanced) {
                        assertCall(ResultSet::rowUpdated, expected, actual, "rowUpdated");
                        assertCall(ResultSet::rowInserted, expected, actual, "rowInserted");
                        assertCall(ResultSet::rowDeleted, expected, actual, "rowDeleted");
                        assertCall(ResultSet::wasNull, expected, actual, "wasNull");
                    }
                }
             }
            result.add(rowData);
            row++;
            if (row > limit) {
                checkExtraRows = false;
                break;
            }
        }
        assertResultSetNavigationState(nativeUrl, expected, actual);

        if (checkExtraRows) {
            assertFalse(expected.next(), format("%s:expected result set has extra rows", message));
            assertFalse(actual.next(), format("%s:actual result set has extra rows", message));
        }

        if (advanced) {
            assertResultSetNavigation(expected, actual);
        }
        return result;
    }


    private static boolean next(ResultSet expected, ResultSet actual) throws SQLException {
        boolean expectedNext = expected.next();
        boolean actualNext = actual.next();
        assertEquals(expectedNext, actualNext);
        return expectedNext;
    }

    private static void assertResultSetNavigationState(String nativeUrl, ResultSet expected, ResultSet actual) throws SQLException {
        if (nativeUrl.startsWith("jdbc:derby")) {
            // Derby throws exception on each isXXX() method: java.sql.SQLException: The 'isXXX' method is only allowed on scroll cursors.
            return;
        }
        assertEquals(expected.isBeforeFirst(), actual.isBeforeFirst());
        assertEquals(expected.isFirst(), actual.isFirst());
        assertEquals(expected.isLast(), actual.isLast());
        assertEquals(expected.isAfterLast(), actual.isAfterLast());
    }

    private static void assertResultSetNavigation(ResultSet expected, ResultSet actual) throws SQLException {
        assertCall(ResultSet::first, expected, actual, "first");
        assertCall(ResultSet::last, expected, actual, "last");
        assertCall(ResultSet::previous, expected, actual, "previous");
        assertCall(rs -> {return rs.absolute(1);}, expected, actual, "absolute(1)");
        assertCall(rs -> {return rs.relative(1);}, expected, actual, "relative(1)");

        assertCall(rs -> {
            rs.beforeFirst();
            return rs.isBeforeFirst();
        }, expected, actual, "beforeFirst");
        assertCall(rs -> {
            rs.afterLast();
            return rs.isAfterLast();
        }, expected, actual, "beforeFirst");
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
            assertEquals(((Number)expected).doubleValue(), ((Number)actual).doubleValue(), 0.001, message);
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


    public static <T, U> void assertCall(ThrowingFunction<T, U, SQLException> f, T nativeObj, T httpObj, String message) throws SQLException {
        assertCall(f, nativeObj, httpObj, message, (e, a) -> {});
    }

    public static <T, U> void assertCall(ThrowingFunction<T, U, SQLException> f, T nativeObj, T httpObj, String message, ThrowingBiConsumer<U, U, SQLException> assertor) throws SQLException {
        U nativeRes = null;
        U httpRes = null;
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
                assertor.accept(nativeRes, httpRes);
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

    public static <T> void assertGettersAndSetters(Collection<Map.Entry<String, Map.Entry<ThrowingFunction<T, ?, SQLException>, ThrowingConsumer<T, SQLException>>>> functions, T nativeObj, T httpObj) throws SQLException {
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

package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingBiConsumer;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.api.Assertions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static com.nosqldriver.util.function.Pair.pair;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AssertUtils {
    enum ResultSetAssertMode {
        CHECK_STATE, CALL_ALL_GETTERS, RANGE_EXCEPTION_MESSAGE,;
    }
    private static final Collection<Class<?>> integerTypes = new HashSet<>(asList(Byte.class, Short.class, Integer.class, Long.class));
    private static final Collection<Class<?>> floatingTypes = new HashSet<>(asList(Float.class, Double.class, BigDecimal.class));
    private static final Map<Integer, Collection<Entry<String, ThrowingBiFunction<ResultSet, Integer, ?, SQLException>>>> typedGettersByIndex = new HashMap<>();
    static {
        typedGettersByIndex.put(Types.BIT, asList(pair("getBoolean", ResultSet::getBoolean), pair("getObject", ResultSet::getObject)));
        typedGettersByIndex.put(Types.TINYINT, asList(pair("getByte", ResultSet::getByte), pair("getShort", ResultSet::getShort), pair("getInt", ResultSet::getInt), pair("getLong", ResultSet::getLong)));
        typedGettersByIndex.put(Types.SMALLINT, asList(pair("getShort", ResultSet::getShort), pair("getInt", ResultSet::getInt), pair("getLong", ResultSet::getLong)));
        typedGettersByIndex.put(Types.INTEGER, asList(pair("getInt", ResultSet::getInt), pair("getLong", ResultSet::getLong)));
        typedGettersByIndex.put(Types.BIGINT, asList(pair("getLong", ResultSet::getLong)));
        typedGettersByIndex.put(Types.FLOAT, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByIndex.put(Types.REAL, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByIndex.put(Types.DOUBLE, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByIndex.put(Types.NUMERIC, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByIndex.put(Types.DECIMAL, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble), pair("getBigDecimal", ResultSet::getBigDecimal)));
        typedGettersByIndex.put(Types.CHAR, asList(pair("getString[0]", (rs, i) -> {String s = rs.getString(i); return s == null ? null : s.length() > 0 ? s.substring(0, 1) : "";})));
        typedGettersByIndex.put(Types.VARCHAR, asList(pair("getString", ResultSet::getString)));
        typedGettersByIndex.put(Types.LONGVARCHAR, asList(pair("getString", ResultSet::getString)));
        typedGettersByIndex.put(Types.DATE, asList(pair("getDate", ResultSet::getDate)));
        typedGettersByIndex.put(Types.TIME, asList(pair("getTime", ResultSet::getTime)));
        typedGettersByIndex.put(Types.TIME_WITH_TIMEZONE, asList(pair("getTime", ResultSet::getTime)));
        typedGettersByIndex.put(Types.TIMESTAMP, asList(pair("getTimestamp", ResultSet::getTimestamp)));
        typedGettersByIndex.put(Types.TIMESTAMP_WITH_TIMEZONE, asList(pair("getTimestamp", ResultSet::getTimestamp)));
        //getters.put(Types.BINARY, ResultSet::getObject);
        //getters.put(Types.VARBINARY, ResultSet::getInt);
        //getters.put(Types.LONGVARBINARY, ResultSet::getInt);
        typedGettersByIndex.put(Types.NULL, asList(pair("getObject", ResultSet::getObject)));
        //getters.put(Types.OTHER, ResultSet::getObject);
        //getters.put(Types.JAVA_OBJECT, ResultSet::getObject);
        //getters.put(Types.DISTINCT, ResultSet::getInt);
        //getters.put(Types.STRUCT, ResultSet::getInt);
        typedGettersByIndex.put(Types.ARRAY, asList(pair("getArray", ResultSet::getArray)));
        typedGettersByIndex.put(Types.BLOB, asList(pair("getBlob", ResultSet::getBlob)));
        typedGettersByIndex.put(Types.CLOB, asList(pair("getClob", ResultSet::getClob)));
        typedGettersByIndex.put(Types.REF, asList(pair("getRef", ResultSet::getRef)));
        //getters.put(Types.DATALINK, ResultSet::getObject);
        typedGettersByIndex.put(Types.BOOLEAN, asList(pair("getBoolean", ResultSet::getBoolean)));
        typedGettersByIndex.put(Types.ROWID, asList(pair("getRowId", ResultSet::getRowId)));
    }

    private static final Collection<Entry<String, ThrowingBiFunction<ResultSet, Integer, ?, SQLException>>> allGettersByIndex = Arrays.asList(
            pair("getBoolean", ResultSet::getBoolean),
            pair("getByte", ResultSet::getByte), pair("getShort", ResultSet::getShort), pair("getInt", ResultSet::getInt), pair("getString", ResultSet::getLong),
            pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble), pair("getBigDecimal", ResultSet::getBigDecimal),
            pair("getString", ResultSet::getString), //returns different string representation for H2: Time
            pair("getNString", ResultSet::getNString), // throws not supported excetion in some drivers
            pair("getDate", ResultSet::getDate), pair("getTime", ResultSet::getTime), pair("getTimestamp", ResultSet::getTimestamp),
            pair("getArray", ResultSet::getArray),
            pair("getBlob", ResultSet::getBlob),
            //pair("getClob", ResultSet::getClob)
            pair("getRef", ResultSet::getRef), pair("getRowId", ResultSet::getRowId)
            //pair("getAsciiStream", ResultSet::getAsciiStream), pair("getBinaryStream", ResultSet::getBinaryStream), pair("getCharacterStream", ResultSet::getCharacterStream), pair("getNCharacterStream", ResultSet::getNCharacterStream), pair("getUnicodeStream", ResultSet::getUnicodeStream)
    );

    private static final Collection<Entry<String, ThrowingBiFunction<ResultSet, String, ?, SQLException>>> allGettersByLabel = Arrays.asList(
            pair("getBoolean", ResultSet::getBoolean),
            pair("getByte", ResultSet::getByte), pair("getShort", ResultSet::getShort), pair("getInt", ResultSet::getInt), pair("getString", ResultSet::getLong),
            pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble), pair("getBigDecimal", ResultSet::getBigDecimal),
            pair("getString", ResultSet::getString), //returns different string representation for H2: Time
            pair("getNString", ResultSet::getNString), // throws not supported excetion in some drivers
            pair("getDate", ResultSet::getDate), pair("getTime", ResultSet::getTime), pair("getTimestamp", ResultSet::getTimestamp),
            pair("getArray", ResultSet::getArray),
            pair("getBlob", ResultSet::getBlob),
            //pair("getClob", ResultSet::getClob) // TODO: or blob, or clob. LOB value cannot be retrieved twice
            pair("getRef", ResultSet::getRef), pair("getRowId", ResultSet::getRowId)
            //pair("getAsciiStream", ResultSet::getAsciiStream), pair("getBinaryStream", ResultSet::getBinaryStream), pair("getCharacterStream", ResultSet::getCharacterStream), pair("getNCharacterStream", ResultSet::getNCharacterStream), pair("getUnicodeStream", ResultSet::getUnicodeStream)
    );

    private static final Map<Integer, Collection<Entry<String, ThrowingBiFunction<ResultSet, String, ?, SQLException>>>> typedGettersByLabel = new HashMap<>();
    static {
        typedGettersByLabel.put(Types.BIT, asList(pair("getBoolean", ResultSet::getBoolean), pair("getObject", ResultSet::getObject)));
        typedGettersByLabel.put(Types.TINYINT, asList(pair("getByte", ResultSet::getByte), pair("getShort", ResultSet::getShort), pair("getInt", ResultSet::getInt), pair("getLong", ResultSet::getLong)));
        typedGettersByLabel.put(Types.SMALLINT, asList(pair("getShort", ResultSet::getShort), pair("getInt", ResultSet::getInt), pair("getLong", ResultSet::getLong)));
        typedGettersByLabel.put(Types.INTEGER, asList(pair("getInt", ResultSet::getInt), pair("getLong", ResultSet::getLong)));
        typedGettersByLabel.put(Types.BIGINT, asList(pair("getLong", ResultSet::getLong)));
        typedGettersByLabel.put(Types.FLOAT, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByLabel.put(Types.REAL, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByLabel.put(Types.DOUBLE, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByLabel.put(Types.NUMERIC, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble)));
        typedGettersByLabel.put(Types.DECIMAL, asList(pair("getFloat", ResultSet::getFloat), pair("getDouble", ResultSet::getDouble), pair("getBigDecimal", ResultSet::getBigDecimal)));
        typedGettersByLabel.put(Types.CHAR, asList(pair("getString[0]", (rs, i) -> {String s = rs.getString(i); return s == null ? null : s.length() > 0 ? s.substring(0, 1) : "";})));
        typedGettersByLabel.put(Types.VARCHAR, asList(pair("getString", ResultSet::getString)));
        typedGettersByLabel.put(Types.LONGVARCHAR, asList(pair("getString", ResultSet::getString)));
        typedGettersByLabel.put(Types.DATE, asList(pair("getDate", ResultSet::getDate)));
        typedGettersByLabel.put(Types.TIME, asList(pair("getTime", ResultSet::getTime)));
        typedGettersByLabel.put(Types.TIME_WITH_TIMEZONE, asList(pair("getTime", ResultSet::getTime)));
        typedGettersByLabel.put(Types.TIMESTAMP, asList(pair("getTimestamp", ResultSet::getTimestamp)));
        typedGettersByLabel.put(Types.TIMESTAMP_WITH_TIMEZONE, asList(pair("getTimestamp", ResultSet::getTimestamp)));
        //getters.put(Types.BINARY, ResultSet::getObject);
        //getters.put(Types.VARBINARY, ResultSet::getInt);
        //getters.put(Types.LONGVARBINARY, ResultSet::getInt);
        typedGettersByLabel.put(Types.NULL, asList(pair("getObject", ResultSet::getObject)));
        //getters.put(Types.OTHER, ResultSet::getObject);
        //getters.put(Types.JAVA_OBJECT, ResultSet::getObject);
        //getters.put(Types.DISTINCT, ResultSet::getInt);
        //getters.put(Types.STRUCT, ResultSet::getInt);
        typedGettersByLabel.put(Types.ARRAY, asList(pair("getArray", ResultSet::getArray)));
        typedGettersByLabel.put(Types.BLOB, asList(pair("getBlob", ResultSet::getBlob)));
        typedGettersByLabel.put(Types.CLOB, asList(pair("getClob", ResultSet::getClob)));
        typedGettersByLabel.put(Types.REF, asList(pair("getRef", ResultSet::getRef)));
        //getters.put(Types.DATALINK, ResultSet::getObject);
        typedGettersByLabel.put(Types.BOOLEAN, asList(pair("getBoolean", ResultSet::getBoolean)));
        typedGettersByLabel.put(Types.ROWID, asList(pair("getRowId", ResultSet::getRowId)));
    }


    /**
     *
     * @param nativeUrl
     * @param expected
     * @param actual
     * @param message
     * @param limit
     * @param mode - exists for performance reasons
     * @return
     * @throws SQLException
     */
    public static Collection<Map<String, Object>> assertResultSet(String nativeUrl, ResultSet expected, ResultSet actual, String message, int limit, Collection<ResultSetAssertMode> mode) throws SQLException {
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

        ThrowingBiConsumer<SQLException, SQLException, SQLException> exceptionAssertor = mode.contains(ResultSetAssertMode.RANGE_EXCEPTION_MESSAGE) ?
                (e, a) -> assertTrue(a.getMessage().contains("is outside of valid range") || a.getMessage().startsWith("Cannot")) :
                (e, a) -> assertEquals(e.getMessage(), a.getMessage(), message);

        int row = 0;
        boolean checkExtraRows = true;

        assertResultSetNavigationState(nativeUrl, expected, actual);
        while (next(expected, actual)) {
            assertResultSetNavigationState(nativeUrl, expected, actual);
            Map<String, Object> rowData = new LinkedHashMap<>();
            for (int i = 1; i <= n; i++) {
                int columnType = emd.getColumnType(i);

                final Collection<Entry<String, ThrowingBiFunction<ResultSet, Integer, ?, SQLException>>> gettersByIndex;
                final Collection<Entry<String, ThrowingBiFunction<ResultSet, String, ?, SQLException>>> gettersByLabel;
                if (mode.contains(ResultSetAssertMode.CALL_ALL_GETTERS)) {
                    gettersByIndex = allGettersByIndex;
                    gettersByLabel = allGettersByLabel;
                } else {
                    gettersByIndex = typedGettersByIndex.getOrDefault(columnType, singleton(pair("getObject", ResultSet::getObject)));
                    gettersByLabel = typedGettersByLabel.getOrDefault(columnType, singleton(pair("getObject", ResultSet::getObject)));
                }

                for (Entry<String, ThrowingBiFunction<ResultSet, Integer, ?, SQLException>> getter : gettersByIndex) {
                    //assertValues(getter.apply(expected, i), getter.apply(actual, i), format("%s:column#%d:%s", message, i, emd.getColumnName(i)));
                    String errorMessage = format("%s:%s(%d):%s:%s", message, getter.getKey(), i, emd.getColumnName(i), emd.getColumnTypeName(i));
                    int j = i;
                    System.out.println("cccccccccccccccccccccccc=" + j + ", " + emd.getColumnName(j) + ", " + getter.getKey());
                    assertCall(rs -> getter.getValue().apply(rs, j), expected, actual, errorMessage, (e, a) -> assertValues(e, a, errorMessage), exceptionAssertor);
                }

                for (Entry<String, ThrowingBiFunction<ResultSet, String, ?, SQLException>> getter : gettersByLabel) {
                    int type = emd.getColumnType(i);
                    if (nativeUrl.startsWith("jdbc:derby") && (type == Types.BLOB || type == Types.CLOB)) {
                        continue; // patch for derby that does not allow to retrieve blobs and clobs more than once. The first time they were retrieved by index.
                    }
                    String label = emd.getColumnLabel(i);
                    assertEquals(expected.findColumn(label), actual.findColumn(label));
//                    Object actualValue = getter.apply(actual, label);
//                    assertValues(getter.apply(expected, label), actualValue, format("%s:column#%s", message, emd.getColumnLabel(i)));

                    String errorMessage = format("%s:%s(%s):%s", message, getter.getKey(), emd.getColumnLabel(i), emd.getColumnTypeName(i));
                    Object actualValue = assertCall(rs -> getter.getValue().apply(rs, label), expected, actual, errorMessage, (e, a) -> assertValues(e, a, errorMessage), exceptionAssertor);

                    rowData.put(label, actualValue);

                    if (mode.contains(ResultSetAssertMode.CHECK_STATE)) {
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

        if (mode.contains(ResultSetAssertMode.CHECK_STATE)) {
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

    public static void assertValues(Object expected, Object actual, String message) throws SQLException {
        if (isInteger(expected) && isInteger(actual)) {
            assertEquals(((Number)expected).longValue(), ((Number)actual).longValue(), message);
        } else if (isFloating(expected) && isFloating(actual)) {
            assertEquals(((Number)expected).doubleValue(), ((Number)actual).doubleValue(), 0.001, message);
        } else if (isArray(expected) && isArray(actual)) {
            assertArrayEquals(expected, actual, message);
        } else if(expected instanceof java.sql.Array && actual instanceof java.sql.Array) {
            assertArrayEquals((java.sql.Array)expected, (java.sql.Array)actual, message);
        } else if (expected instanceof InputStream && actual instanceof InputStream) {
            try {
                Assertions.assertArrayEquals(((InputStream) expected).readAllBytes(), ((InputStream) actual).readAllBytes());
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        } else if (expected instanceof Reader && actual instanceof Reader) {
            assertEquals(readAll((Reader)expected), readAll((Reader)actual));
        } else if (expected instanceof Blob && actual instanceof Blob) {
            //Assertions.assertArrayEquals(getBytes((Blob)expected), getBytes((Blob)actual));
            assertCall(o -> getBytes((Blob)o), expected, actual, "blob", Assertions::assertArrayEquals, (e1, e2) -> {});
        } else if (expected instanceof NClob && actual instanceof NClob) {
            assertEquals(getString((NClob) expected), getString((NClob) actual));
        } else if (expected instanceof Clob && actual instanceof Clob) {
            assertEquals(getString((Clob)expected), getString((Clob)actual));
        } else if (!(expected instanceof Timestamp) && actual instanceof Timestamp) {
            assertNotNull(expected);
            String expStr = expected.toString();
            try {
                assertEquals(new SimpleDateFormat("yyyy-MM-dd HH:mm:ssX").parse(expStr).getTime(), ((Timestamp) actual).getTime());
            } catch (ParseException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            assertEquals(expected, actual, message);
        }
    }

    private static String readAll(Reader reader) {
        return reader == null ? null : new BufferedReader(reader).lines().collect(Collectors.joining(System.lineSeparator()));
    }

    public static boolean isInteger(Object obj) {
        return obj != null && integerTypes.contains(obj.getClass());
    }

    private static byte[] getBytes(Blob blob) throws SQLException {
        return blob.getBytes(1, (int)blob.length());
    }

    private static String getString(Clob clob) throws SQLException {
        return clob.getSubString(1, (int)clob.length());
    }

    private static String getString(NClob clob) throws SQLException {
        return clob.getSubString(1, (int)clob.length());
    }

    public static boolean isFloating(Object obj) {
        return obj != null && floatingTypes.contains(obj.getClass());
    }

    public static boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
    }

    public static void assertArrayEquals(java.sql.Array expected, java.sql.Array actual, String message) {
        try {
            assertArrayEquals(expected.getArray(), actual.getArray(), message);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void assertArrayEquals(Object expected, Object actual, String message) throws SQLException {
        if (expected == actual) {
            return;
        }
        if (expected == null) {
            assertNull(actual);
        } else {
            assertNotNull(actual);
        }

        int nExp = Array.getLength(expected);
        int nAct = Array.getLength(actual);
        assertEquals(nExp, nAct);

        for (int i = 0; i < nExp; i++) {
            assertValues(Array.get(expected, i), Array.get(actual, i), message + " #" + i);
        }
    }

    public static <T, U> void assertCall(ThrowingFunction<T, U, SQLException> f, T nativeObj, T httpObj, String message) throws SQLException {
        assertCall(f, nativeObj, httpObj, message, (e, a) -> {}, (e, a) -> assertEquals(e.getMessage(), a.getMessage(), message));
    }

    public static <T, U> U assertCall(ThrowingFunction<T, U, SQLException> f, T nativeObj, T httpObj, String message, ThrowingBiConsumer<U, U, SQLException> resultAssertor, ThrowingBiConsumer<SQLException, SQLException, SQLException> exceptionAssertor) throws SQLException {
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
                resultAssertor.accept(nativeRes, httpRes);
            }
        } else if (!(nativeEx instanceof SQLFeatureNotSupportedException)) { // some getters throw SQLFeatureNotSupportedException while we can handle it at client side, so we ignore this case
//            if (httpEx == null) {
//                f.apply(httpObj);
//            }
            assertNotNull(httpEx, message);
            //assertEquals(nativeEx.getMessage(), httpEx.getMessage(), message);
            exceptionAssertor.accept(nativeEx, httpEx);
        }
        return httpRes;
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

    public static <T> void assertGettersAndSetters(Collection<Entry<String, Entry<ThrowingFunction<T, ?, SQLException>, ThrowingConsumer<T, SQLException>>>> functions, T nativeObj, T httpObj) throws SQLException {
        for (Entry<String, Entry<ThrowingFunction<T, ?, SQLException>, ThrowingConsumer<T, SQLException>>> function : functions) {
            String name = function.getKey();
            ThrowingFunction<T, ?, SQLException> getter = function.getValue().getKey();
            ThrowingConsumer<T, SQLException> setter = function.getValue().getValue();
            assertCall(getter, nativeObj, httpObj, name); // first call getter
            assertCall(setter, nativeObj, httpObj, name); // now call corresponding setter
            assertCall(getter, nativeObj, httpObj, name); // call getter again to be sure that setter worked
        }
    }
}

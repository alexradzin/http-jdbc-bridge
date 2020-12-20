package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingBiConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map.Entry;

import static com.nosqldriver.jdbc.http.AssertUtils.assertCall;
import static java.lang.String.format;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class ArrayControllerTest extends ControllerTestBase {
    private static class ResultAssertor implements ThrowingBiConsumer<Object, Object, SQLException> {
        private final String name;

        private ResultAssertor(String name) {
            this.name = name;
        }

        @Override
        public void accept(Object nativeRes, Object httpRes) throws SQLException {
            if (nativeRes.getClass().isArray()) {
                assertEquals(java.lang.reflect.Array.getLength(nativeRes), java.lang.reflect.Array.getLength(httpRes), name);
                int n = java.lang.reflect.Array.getLength(httpRes);
                for (int i = 0; i < n; i++) {
                    Object nativeElement = java.lang.reflect.Array.get(nativeRes, i);
                    Object httpElement = java.lang.reflect.Array.get(httpRes, i);
                    assertEquals(nativeElement, httpElement);
                }
            } else {
                assertEquals(nativeRes, httpRes, name);
            }
        }
    }

    private static class ResultSetAssertor implements ThrowingBiConsumer<ResultSet, ResultSet, SQLException> {
        private final String nativeUrl;
        private final String message;
        private final int limit;

        private ResultSetAssertor(String nativeUrl, String message, int limit) {
            this.nativeUrl = nativeUrl;
            this.message = message;
            this.limit = limit;
        }

        @Override
        public void accept(ResultSet expected, ResultSet actual) throws SQLException {
            AssertUtils.assertResultSet(nativeUrl, expected, actual, message, limit);
        }
    }

    private Array nativeArray;
    private Array httpArray;

    private void create(String nativeUrl, String typeName, Object[] elements) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        SQLException nativeEx = null;
        SQLException httpEx = null;

        try {
            nativeArray = nativeConn.createArrayOf(typeName, elements);
        } catch (SQLException e) {
            nativeEx = e;
        }

        try {
            httpArray = httpConn.createArrayOf(typeName, elements);
        } catch (SQLException e) {
            httpEx = e;
        }

        if (nativeEx == null) {
            if (nativeArray == null) {
                assertNull(nativeArray);
            } else {
                assertNotNull(httpArray);
            }
        } else {
            assertNotNull(httpEx);
            assertEquals(nativeEx.getMessage(), httpEx.getMessage());
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getters(String nativeUrl) throws SQLException {
        create(nativeUrl, "int", new Object[0]);
        if (nativeArray == null) {
            assertNull(httpArray);
            return;
        }

        Collection<Entry<String, ThrowingFunction<Array, Object, SQLException>>> functions = Arrays.asList(
                new SimpleEntry<>("getArray", Array::getArray),
                new SimpleEntry<>("getBaseType", Array::getBaseType),
                new SimpleEntry<>("getBaseTypeName", Array::getBaseTypeName),
                new SimpleEntry<>("free", a -> {a.free(); return true;})
        );

        for (Entry<String, ThrowingFunction<Array, Object, SQLException>> e : functions) {
            assertCall(e.getValue(), nativeArray, httpArray, e.getKey());
            //assertCall(e.getKey(), e.getValue());
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getIntArray1(String nativeUrl) throws SQLException {
        String comment = "getArray()=>[1]";
        getArray(nativeUrl, "int", new Object[] {1}, comment, new ResultAssertor(comment), Array::getArray);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getStringArray1(String nativeUrl) throws SQLException {
        String comment = "getArray()=>['hello']";
        getArray(nativeUrl, "string", new Object[] {"hello"}, comment, new ResultAssertor(comment), Array::getArray);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getIntArrayN(String nativeUrl) throws SQLException {
        String comment = "getArray()=>[10, 20, 30, 40, 50]";
        getArray(nativeUrl, "int", new Object[] {10, 20, 30, 40, 50}, comment, new ResultAssertor(comment), Array::getArray);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getIntArrayNSlice(String nativeUrl) throws SQLException {
        String comment = "getArray()=>[10, 20, 30, 40, 50]";
        getArray(nativeUrl, "int", new Object[] {10, 20, 30, 40, 50}, comment,
                new ResultAssertor(comment),
                a -> a.getArray(1, 5),
                a -> a.getArray(2, 4),
                a -> a.getArray(0, 5),
                a -> a.getArray(1, 6),
                a -> a.getArray(1, 5, singletonMap("int", Integer.class)),
                a -> a.getArray(1, 5, singletonMap("int", Long.class)),
                a -> a.getArray(1, 5, singletonMap("int", Short.class)),
                a -> a.getArray(singletonMap("int", Short.class))
        );
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void resultSet(String nativeUrl) throws SQLException {
        String comment = "ResultSet";
        getArray(nativeUrl, "int", new Object[] {10, 20, 30, 40, 50}, comment, new ResultSetAssertor(nativeUrl, comment, 10),
                Array::getResultSet,
                a -> a.getResultSet(Collections.emptyMap()),
                a -> a.getResultSet(1, 1),
                a -> a.getResultSet(1, 1, Collections.emptyMap())
        );
    }


    @SafeVarargs
    private <T> void getArray(String nativeUrl, String typeName, Object[] array, String comment,
                          ThrowingBiConsumer<T, T, SQLException> assertor,
                          ThrowingFunction<Array, T, SQLException> ... functions) throws SQLException {
        create(nativeUrl, typeName, array);
        if (nativeArray == null) {
            assertNull(httpArray);
            return;
        }

        for (ThrowingFunction<Array, T, SQLException> function : functions) {
            assertCall(function, nativeArray, httpArray, comment, assertor);
        }
    }
}

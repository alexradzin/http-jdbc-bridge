package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Collections;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StructControllerTest extends ControllerTestBase {
    @BeforeEach
    @Override
    void initDb(TestInfo testInfo) throws SQLException {
        String nativeUrl = "jdbc:mock";
        nativeConn = DriverManager.getConnection(nativeUrl);
        httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
    }

    @Test
    void test() throws SQLException {
        Struct struct = mock(Struct.class);
        when(nativeConn.createStruct(any(String.class), any(Object[].class))).thenReturn(struct);

        Struct nativeStruct = nativeConn.createStruct("person", new Object[] {"first_name", "last_name"});
        Struct httpStruct = httpConn.createStruct("person", new Object[] {"first_name", "last_name"});

        assertEquals(nativeStruct.getSQLTypeName(), httpStruct.getSQLTypeName());
        assertArrayEquals(nativeStruct.getAttributes(), httpStruct.getAttributes());
        assertArrayEquals(nativeStruct.getAttributes(Collections.emptyMap()), httpStruct.getAttributes(Collections.emptyMap()));
        assertArrayEquals(nativeStruct.getAttributes(Collections.singletonMap("first_name", String.class)), httpStruct.getAttributes(Collections.singletonMap("first_name", String.class)));
    }
}

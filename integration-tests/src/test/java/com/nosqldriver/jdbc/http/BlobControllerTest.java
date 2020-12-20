package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingConsumer;
import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import static com.nosqldriver.jdbc.http.AssertUtils.assertCall;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

class BlobControllerTest extends ControllerTestBase {
    private Blob nativeBlob;
    private Blob httpBlob;

    private void create(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        SQLException nativeEx = null;
        SQLException httpEx = null;

        try {
            nativeBlob = nativeConn.createBlob();
        } catch (SQLException e) {
            nativeEx = e;
        }

        try {
            httpBlob = httpConn.createBlob();
        } catch (SQLException e) {
            httpEx = e;
        }

        if (nativeEx == null) {
            if (nativeBlob == null) {
                assertNull(httpBlob);
            } else {
                assertNotNull(httpBlob);
            }
        } else {
            assertNotNull(httpEx);
            assertEquals(nativeEx.getMessage(), httpEx.getMessage());
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void bytes(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeBlob == null) {
            return;
        }

        byte[] xml = "<hello/>".getBytes();
        httpBlob.setBytes(1, xml);
        assertArrayEquals(xml, httpBlob.getBytes(1, xml.length));
        assertArrayEquals("ello/>".getBytes(), httpBlob.getBytes(3, xml.length - 2));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void positionAndLength(String nativeUrl) throws SQLException {
        Collection<Entry<String, ThrowingConsumer<Blob, SQLException>>> initializers = Arrays.asList(
                new SimpleEntry<>("no-op", blob -> {}),
                new SimpleEntry<>("empty", blob -> blob.setBytes(1, new byte[0])),
                new SimpleEntry<>("string", blob -> blob.setBytes(1, "hello".getBytes()))
        );

        for (Entry<String, ThrowingConsumer<Blob, SQLException>> initializer : initializers) {
            String name = initializer.getKey();
            ThrowingConsumer<Blob, SQLException> i = initializer.getValue();
            positionAndLength(nativeUrl, i, name);
        }
    }

    private void positionAndLength(String nativeUrl, ThrowingConsumer<Blob, SQLException> initializer, String initializerName) throws SQLException {
        create(nativeUrl);
        if (nativeBlob == null) {
            return;
        }
        initializer.accept(nativeBlob);
        initializer.accept(httpBlob);

//        Blob mockedBlob = Mockito.mock(Blob.class);
//        when(mockedBlob.getBytes(anyLong(), anyInt())).thenReturn(new byte[0]);

        Collection<Entry<String, ThrowingFunction<Blob, Long,  SQLException>>> functions = Arrays.asList(
            new SimpleEntry<>(initializerName + ":length", Blob::length),
            new SimpleEntry<>(initializerName + ":position([], 1)", blob -> blob.position(new byte[0], 1)),
            new SimpleEntry<>(initializerName + ":position([], 2)", blob -> blob.position(new byte[0], 2)),
            new SimpleEntry<>(initializerName + ":position([1,2,3], 1)", blob -> blob.position(new byte[] {1,2,3}, 1)),
            new SimpleEntry<>(initializerName + ":position([1,2,3], 5)", blob -> blob.position(new byte[] {1,2,3}, 5)),
            new SimpleEntry<>(initializerName + ":position([1,2,3], 5)", blob -> blob.position("hello".getBytes(), 1)),
            new SimpleEntry<>(initializerName + ":position(blob, 1)", blob -> blob.position(blob, 1))
            //new SimpleEntry<>(initializerName + ":position(blob, 1)", blob -> blob.position(mockedBlob, 1))
        );

        for (Entry<String, ThrowingFunction<Blob, Long, SQLException>> function : functions) {
            String name = function.getKey();
            ThrowingFunction<Blob, Long, SQLException> f = function.getValue();
            assertCall(f, nativeBlob, httpBlob, name);
        }
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void stream(String nativeUrl) throws SQLException, IOException {
        create(nativeUrl);
        if (nativeBlob == null) {
            return;
        }

        byte[] xml = "<hello/>".getBytes();

        try {
            OutputStream os = httpBlob.setBinaryStream(1);
            os.write(xml);
            os.flush();
            os.close();

            try(InputStream in = httpBlob.getBinaryStream()) {
                assertArrayEquals(xml, in.readAllBytes());
            }
            assertArrayEquals(xml, httpBlob.getBinaryStream(1, xml.length).readAllBytes());
            assertArrayEquals("hello".getBytes(), httpBlob.getBinaryStream(2, xml.length - 3).readAllBytes());
        } catch (SQLFeatureNotSupportedException e) {
            // ignore. Unsupported...
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void setBytesAndStream(String nativeUrl) throws IOException, SQLException {
        byte[] input = "<hello/>".getBytes();
        stream2(nativeUrl, input, bytes -> httpBlob.setBytes(1, input));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void setBytesWithOffsetAndStream(String nativeUrl) throws IOException, SQLException {
        byte[] input = "<hello/>".getBytes();
        stream2(nativeUrl, input, bytes -> httpBlob.setBytes(1, input, 0, input.length));
    }

    private void stream2(String nativeUrl, byte[] bytes, ThrowingConsumer<byte[], SQLException> setBytes) throws SQLException, IOException {
        if ("jdbc:h2:mem:test".equals(nativeUrl)) {
            return; // does not work for h2
        }
        create(nativeUrl);
        if (nativeBlob == null) {
            return;
        }

        setBytes.accept(bytes);
        try {
            OutputStream os = httpBlob.setBinaryStream(2);
            os.write(bytes);
            os.flush();
            os.close();

            assertArrayEquals(bytes, httpBlob.getBinaryStream(2, bytes.length).readAllBytes());
        } catch (SQLFeatureNotSupportedException e) {
            // ignore. Unsupported...
        }
    }



    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void free(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeBlob == null) {
            return;
        }
        bytes(nativeUrl);
        httpBlob.free();
        assertThrows(SQLException.class, () -> httpBlob.getBytes(1, 8)); // already closed
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void truncate(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeBlob == null) {
            return;
        }
        bytes(nativeUrl);
        try {
            httpBlob.truncate(6);
            try {
                assertEquals("<hello", new String(httpBlob.getBytes(1, 8)));
            } catch (SQLException e) {
                // only derby allows getting bytes after truncation
                // all others throw SQLException because the blob is already closed
            }
        } catch (SQLFeatureNotSupportedException e) {
            // h2 does not support truncate
        }
    }
}
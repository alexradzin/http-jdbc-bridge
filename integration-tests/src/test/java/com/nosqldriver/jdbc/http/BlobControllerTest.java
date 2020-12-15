package com.nosqldriver.jdbc.http;

import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

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
                assertNull(nativeBlob);
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
    void stream2(String nativeUrl) throws SQLException, IOException {
        if ("jdbc:h2:mem:test".equals(nativeUrl)) {
            return; // does not work for h2
        }
        create(nativeUrl);
        if (nativeBlob == null) {
            return;
        }

        httpBlob.setBytes(1, "<hello/>".getBytes()); // needed for some databases; otherwise setBinaryStream(2) throws exception
        byte[] xml = "<hello/>".getBytes();

        try {
            OutputStream os = httpBlob.setBinaryStream(2);
            os.write(xml);
            os.flush();
            os.close();

            assertArrayEquals(xml, httpBlob.getBinaryStream(2, xml.length).readAllBytes());
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
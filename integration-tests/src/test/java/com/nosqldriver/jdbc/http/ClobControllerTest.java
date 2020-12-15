package com.nosqldriver.jdbc.http;

import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class ClobControllerTest extends ControllerTestBase {
    private Clob nativeClob;
    private Clob httpClob;

    private void create(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        SQLException nativeEx = null;
        SQLException httpEx = null;

        try {
            nativeClob = nativeConn.createClob();
        } catch (SQLException e) {
            nativeEx = e;
        }

        try {
            httpClob = httpConn.createClob();
        } catch (SQLException e) {
            httpEx = e;
        }

        if (nativeEx == null) {
            if (nativeClob == null) {
                assertNull(nativeClob);
            } else {
                assertNotNull(httpClob);
            }
        } else {
            assertNotNull(httpEx);
            assertEquals(nativeEx.getMessage(), httpEx.getMessage());
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void string(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeClob == null) {
            return;
        }

        String xml = "<hello/>";
        httpClob.setString(1, xml);
        assertEquals(xml, httpClob.getSubString(1, xml.length()));
        assertEquals("hello", httpClob.getSubString(2, xml.length() - 3));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void asciiStream(String nativeUrl) throws SQLException, IOException {
        create(nativeUrl);
        if (nativeClob == null) {
            return;
        }

        try {
            String xml = "<hello/>";
            httpClob.setString(1, xml);
            OutputStream os = httpClob.setAsciiStream(1);
            os.write(xml.getBytes());
            os.flush();
            os.close();

            try (InputStream in = httpClob.getAsciiStream()) {
                String xml2 = new String(in.readAllBytes());
                assertEquals(xml, xml2);
            }
        } catch (SQLException e) {
            if (!(e instanceof SQLFeatureNotSupportedException || e.getMessage().contains("Feature not supported"))) {
                throw e;
            }
            // ignore. Unsupported...
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void free(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeClob == null) {
            return;
        }
        string(nativeUrl);
        httpClob.free();
        try {
            assertNull(httpClob.getSubString(1, 8));
        } catch(SQLException e) {
            // all but mysql throws exception here because clob is already field
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void truncate(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeClob == null) {
            return;
        }
        string(nativeUrl);
        try {
            httpClob.truncate(6);
            try {
                assertEquals("<hello", httpClob.getSubString(1, 8));
            } catch (SQLException e) {
                // only derby allows getting bytes after truncation
                // all others throw SQLException because the blob is already closed
            }
        } catch (SQLFeatureNotSupportedException e) {
            // h2 does not support truncate
        }
    }

}

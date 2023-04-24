package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingFunction;
import org.junit.jupiter.params.ParameterizedTest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
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
        assertEquals(xml.length(), httpClob.length());
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void string2(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeClob == null) {
            return;
        }

        String xml = "<hello/>";
        httpClob.setString(1, xml, 1, 5);
        assertEquals("hello", httpClob.getSubString(1, 5));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void position(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeClob == null) {
            return;
        }
        if ("jdbc:h2:mem:test".equals(nativeUrl)) {
            return; // H2 does not support this feature
        }

        String xml = "<hello/>";
        httpClob.setString(1, xml);
        assertEquals(1, httpClob.position(xml, 1));
        assertEquals(2, httpClob.position("hello", 1));
        assertEquals(1, httpClob.position(httpClob, 1));
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
    void characterStream(String nativeUrl) throws SQLException, IOException {
        characterStream(nativeUrl, "<hello/>", Clob::getCharacterStream);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void characterStream1(String nativeUrl) throws SQLException, IOException {
        characterStream(nativeUrl, "bye", clob -> clob.getCharacterStream(1, "bye".length()));
    }

    private void characterStream(String nativeUrl, String in, ThrowingFunction<Clob, Reader, SQLException> characterStreamGetter) throws SQLException, IOException {
        create(nativeUrl);
        if (nativeClob == null) {
            return;
        }

        try {
            httpClob.setString(1, in);
            Writer writer = httpClob.setCharacterStream(1);
            writer.write(in.toCharArray());
            writer.flush();
            writer.close();

            try (Reader reader = characterStreamGetter.apply(httpClob)) {
                char[] out = new char[in.length()];
                assertEquals(out.length, reader.read(out));
                String xml2 = new String(out);
                assertEquals(in, xml2);
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

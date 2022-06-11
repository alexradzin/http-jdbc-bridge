package com.nosqldriver.jdbc.http;

import org.junit.jupiter.params.ParameterizedTest;

import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class SQLXMLControllerTest extends ControllerTestBase {
    private SQLXML nativeSQLXML;
    private SQLXML httpSQLXML;

    private void create(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        SQLException nativeEx = null;
        SQLException httpEx = null;

        try {
            nativeSQLXML = nativeConn.createSQLXML();
        } catch (SQLException e) {
            nativeEx = e;
        }

        try {
            httpSQLXML = httpConn.createSQLXML();
        } catch (SQLException e) {
            httpEx = e;
        }

        if (nativeEx == null) {
            if (nativeSQLXML == null) {
                assertNull(nativeSQLXML);
            } else {
                assertNotNull(httpSQLXML);
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
        if (nativeSQLXML == null) {
            return;
        }

        String xml = "<hello/>";
        httpSQLXML.setString(xml);
        assertTrue(httpSQLXML.getString().endsWith(xml));
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void characterStream(String nativeUrl) throws SQLException, IOException {
        create(nativeUrl);
        if (nativeSQLXML == null) {
            return;
        }

        String xml = "<hello/>";
        Writer w = httpSQLXML.setCharacterStream();
        w.append(xml);
        w.flush();
        w.close();

        if (nativeUrl.startsWith("jdbc:mysql:")) {
            // for some reason mysql driver does not allow to read from stream and throws
            // java.sql.SQLException: Can't perform requested operation after getResult() has been called to write XML data
            return;
        }
        Reader r = httpSQLXML.getCharacterStream();
        BufferedReader br = new BufferedReader(r);
        String xml2 = br.readLine();
        br.close();
        assertEquals(xml, xml2);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void binaryStream(String nativeUrl) throws SQLException, IOException {
        create(nativeUrl);
        if (nativeSQLXML == null) {
            return;
        }

        byte[] xml = "<hello/>".getBytes();
        OutputStream os = httpSQLXML.setBinaryStream();
        os.write(xml);
        os.flush();
        os.close();

        if (nativeUrl.startsWith("jdbc:mysql:")) {
            // for some reason mysql driver does not allow to read from stream and throws
            // java.sql.SQLException: Can't perform requested operation after getResult() has been called to write XML data
            return;
        }
        InputStream in = httpSQLXML.getBinaryStream();
        byte[] xml2 = in.readAllBytes();
        assertArrayEquals(xml, xml2);
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void getSourceSetResult(String nativeUrl) throws SQLException {
        create(nativeUrl);
        if (nativeSQLXML == null) {
            return;
        }
        assertThrows(SQLFeatureNotSupportedException.class, () -> httpSQLXML.getSource(DOMSource.class));
        assertThrows(SQLFeatureNotSupportedException.class, () -> httpSQLXML.setResult(DOMResult.class));
    }
}

package com.nosqldriver.jdbc.http;

import com.nosqldriver.jdbc.http.model.EntityProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class AutoClosableTest extends ControllerTestBase {
    @BeforeEach
    void beforeEach(TestInfo testInfo) throws SQLException {
        super.beforeEach(testInfo);
        String nativeUrl = testInfo.getDisplayName();
        nativeConn.createStatement().executeUpdate(sqlScript(db(nativeUrl), "create.table.all-types.sql"));
    }

    @AfterEach
    void cleanDb(TestInfo testInfo) throws SQLException {
        String nativeUrl = testInfo.getDisplayName();
        nativeConn.createStatement().executeUpdate(sqlScript(db(nativeUrl), "drop.table.all-types.sql"));
        super.cleanDb();
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void closeStatementAndItsResultSet(@SuppressWarnings("unused") String nativeUrl) throws SQLException {
        Statement statement = httpConn.createStatement();
        getProxyKey(statement);
        ResultSet rs = statement.executeQuery("select * from test_all_types");
        rs.next();
        assertFalse(statement.isClosed());
        assertFalse(rs.isClosed());

        String statementKey = getProxyKey(statement);
        String rsKey = getProxyKey(rs);
        assertTrue(attributes.containsKey(statementKey));
        assertTrue(attributes.containsKey(rsKey));

        statement.close();
        assertTrue(statement.isClosed());
        assertTrue(rs.isClosed());
        assertFalse(attributes.containsKey(statementKey));
        assertFalse(attributes.containsKey(rsKey));
    }

    private String getProxyKey(Object obj) {
        String[] urlParts = ((EntityProxy)obj).getEntityUrl().split("/");
        return format("%s@%s", urlParts[urlParts.length - 2], urlParts[urlParts.length - 1]);
    }
}

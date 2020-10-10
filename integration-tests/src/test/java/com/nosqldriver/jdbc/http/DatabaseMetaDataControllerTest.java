package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingFunction;
import com.nosqldriver.util.function.ThrowingQuadraFunction;
import com.nosqldriver.util.function.ThrowingTriFunction;
import org.junit.jupiter.params.ParameterizedTest;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map.Entry;

import static com.nosqldriver.jdbc.http.AssertUtils.assertCall;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_PLACEHOLDER;

public class DatabaseMetaDataControllerTest extends ControllerTestBase {
    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void primitiveGetters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        DatabaseMetaData httpMd = httpConn.getMetaData();
        DatabaseMetaData nativeMd = nativeConn.getMetaData();

        Collection<Entry<String, ThrowingFunction<DatabaseMetaData, ?, SQLException>>> getters = Arrays.asList(
                new SimpleEntry<>("allProceduresAreCallable", DatabaseMetaData::allProceduresAreCallable),
                new SimpleEntry<>("allTablesAreSelectable", DatabaseMetaData::allTablesAreSelectable),
                new SimpleEntry<>("getURL", DatabaseMetaData::getURL),
                new SimpleEntry<>("getUserName", DatabaseMetaData::getUserName),
                new SimpleEntry<>("isReadOnly", DatabaseMetaData::isReadOnly),
                new SimpleEntry<>("nullsAreSortedHigh", DatabaseMetaData::nullsAreSortedHigh),
                new SimpleEntry<>("nullsAreSortedLow", DatabaseMetaData::nullsAreSortedLow),
                new SimpleEntry<>("nullsAreSortedAtStart", DatabaseMetaData::nullsAreSortedAtStart),
                new SimpleEntry<>("nullsAreSortedAtEnd", DatabaseMetaData::nullsAreSortedAtEnd),
                new SimpleEntry<>("getDatabaseProductName", DatabaseMetaData::getDatabaseProductName),
                new SimpleEntry<>("getDatabaseProductVersion", DatabaseMetaData::getDatabaseProductVersion),
                new SimpleEntry<>("getDriverName", DatabaseMetaData::getDriverName),
                new SimpleEntry<>("getDriverVersion", DatabaseMetaData::getDriverVersion),
                new SimpleEntry<>("usesLocalFiles", DatabaseMetaData::usesLocalFiles),
                new SimpleEntry<>("usesLocalFilePerTable", DatabaseMetaData::usesLocalFilePerTable),
                new SimpleEntry<>("supportsMixedCaseIdentifiers", DatabaseMetaData::supportsMixedCaseIdentifiers),
                new SimpleEntry<>("storesUpperCaseIdentifiers", DatabaseMetaData::storesUpperCaseIdentifiers),
                new SimpleEntry<>("storesLowerCaseIdentifiers", DatabaseMetaData::storesLowerCaseIdentifiers),
                new SimpleEntry<>("storesMixedCaseIdentifiers", DatabaseMetaData::storesMixedCaseIdentifiers),
                new SimpleEntry<>("supportsMixedCaseQuotedIdentifiers", DatabaseMetaData::supportsMixedCaseQuotedIdentifiers),
                new SimpleEntry<>("storesUpperCaseQuotedIdentifiers", DatabaseMetaData::storesUpperCaseQuotedIdentifiers),
                new SimpleEntry<>("storesLowerCaseQuotedIdentifiers", DatabaseMetaData::storesLowerCaseQuotedIdentifiers),
                new SimpleEntry<>("storesMixedCaseQuotedIdentifiers", DatabaseMetaData::storesMixedCaseQuotedIdentifiers),
                new SimpleEntry<>("getIdentifierQuoteString", DatabaseMetaData::getIdentifierQuoteString),
                new SimpleEntry<>("getSQLKeywords", DatabaseMetaData::getSQLKeywords),
                new SimpleEntry<>("getNumericFunctions", DatabaseMetaData::getNumericFunctions),
                new SimpleEntry<>("getStringFunctions", DatabaseMetaData::getStringFunctions),
                new SimpleEntry<>("getSystemFunctions", DatabaseMetaData::getSystemFunctions),
                new SimpleEntry<>("getTimeDateFunctions", DatabaseMetaData::getTimeDateFunctions),
                new SimpleEntry<>("getSearchStringEscape", DatabaseMetaData::getSearchStringEscape),
                new SimpleEntry<>("getExtraNameCharacters", DatabaseMetaData::getExtraNameCharacters),
                new SimpleEntry<>("supportsAlterTableWithAddColumn", DatabaseMetaData::supportsAlterTableWithAddColumn),
                new SimpleEntry<>("supportsAlterTableWithDropColumn", DatabaseMetaData::supportsAlterTableWithDropColumn),
                new SimpleEntry<>("supportsColumnAliasing", DatabaseMetaData::supportsColumnAliasing),
                new SimpleEntry<>("nullPlusNonNullIsNull", DatabaseMetaData::nullPlusNonNullIsNull),
                new SimpleEntry<>("supportsConvert", DatabaseMetaData::supportsConvert),
                new SimpleEntry<>("supportsTableCorrelationNames", DatabaseMetaData::supportsTableCorrelationNames),
                new SimpleEntry<>("supportsDifferentTableCorrelationNames", DatabaseMetaData::supportsDifferentTableCorrelationNames),
                new SimpleEntry<>("supportsExpressionsInOrderBy", DatabaseMetaData::supportsExpressionsInOrderBy),
                new SimpleEntry<>("supportsOrderByUnrelated", DatabaseMetaData::supportsOrderByUnrelated),
                new SimpleEntry<>("supportsGroupBy", DatabaseMetaData::supportsGroupBy),
                new SimpleEntry<>("supportsGroupByUnrelated", DatabaseMetaData::supportsGroupByUnrelated),
                new SimpleEntry<>("supportsGroupByBeyondSelect", DatabaseMetaData::supportsGroupByBeyondSelect),
                new SimpleEntry<>("supportsLikeEscapeClause", DatabaseMetaData::supportsLikeEscapeClause),
                new SimpleEntry<>("supportsMultipleResultSets", DatabaseMetaData::supportsMultipleResultSets),
                new SimpleEntry<>("supportsMultipleTransactions", DatabaseMetaData::supportsMultipleTransactions),
                new SimpleEntry<>("supportsNonNullableColumns", DatabaseMetaData::supportsNonNullableColumns),
                new SimpleEntry<>("supportsMinimumSQLGrammar", DatabaseMetaData::supportsMinimumSQLGrammar),
                new SimpleEntry<>("supportsCoreSQLGrammar", DatabaseMetaData::supportsCoreSQLGrammar),
                new SimpleEntry<>("supportsExtendedSQLGrammar", DatabaseMetaData::supportsExtendedSQLGrammar),
                new SimpleEntry<>("supportsANSI92EntryLevelSQL", DatabaseMetaData::supportsANSI92EntryLevelSQL),
                new SimpleEntry<>("supportsANSI92IntermediateSQL", DatabaseMetaData::supportsANSI92IntermediateSQL),
                new SimpleEntry<>("supportsANSI92FullSQL", DatabaseMetaData::supportsANSI92FullSQL),
                new SimpleEntry<>("supportsIntegrityEnhancementFacility", DatabaseMetaData::supportsIntegrityEnhancementFacility),
                new SimpleEntry<>("supportsOuterJoins", DatabaseMetaData::supportsOuterJoins),
                new SimpleEntry<>("supportsFullOuterJoins", DatabaseMetaData::supportsFullOuterJoins),
                new SimpleEntry<>("supportsLimitedOuterJoins", DatabaseMetaData::supportsLimitedOuterJoins),
                new SimpleEntry<>("getSchemaTerm", DatabaseMetaData::getSchemaTerm),
                new SimpleEntry<>("getProcedureTerm", DatabaseMetaData::getProcedureTerm),
                new SimpleEntry<>("getCatalogTerm", DatabaseMetaData::getCatalogTerm),
                new SimpleEntry<>("isCatalogAtStart", DatabaseMetaData::isCatalogAtStart),
                new SimpleEntry<>("getCatalogSeparator", DatabaseMetaData::getCatalogSeparator),
                new SimpleEntry<>("supportsSchemasInDataManipulation", DatabaseMetaData::supportsSchemasInDataManipulation),
                new SimpleEntry<>("supportsSchemasInProcedureCalls", DatabaseMetaData::supportsSchemasInProcedureCalls),
                new SimpleEntry<>("supportsSchemasInTableDefinitions", DatabaseMetaData::supportsSchemasInTableDefinitions),
                new SimpleEntry<>("supportsSchemasInIndexDefinitions", DatabaseMetaData::supportsSchemasInIndexDefinitions),
                new SimpleEntry<>("supportsSchemasInPrivilegeDefinitions", DatabaseMetaData::supportsSchemasInPrivilegeDefinitions),
                new SimpleEntry<>("supportsCatalogsInDataManipulation", DatabaseMetaData::supportsCatalogsInDataManipulation),
                new SimpleEntry<>("supportsCatalogsInProcedureCalls", DatabaseMetaData::supportsCatalogsInProcedureCalls),
                new SimpleEntry<>("supportsCatalogsInTableDefinitions", DatabaseMetaData::supportsCatalogsInTableDefinitions),
                new SimpleEntry<>("supportsCatalogsInIndexDefinitions", DatabaseMetaData::supportsCatalogsInIndexDefinitions),
                new SimpleEntry<>("supportsCatalogsInPrivilegeDefinitions", DatabaseMetaData::supportsCatalogsInPrivilegeDefinitions),
                new SimpleEntry<>("supportsPositionedDelete", DatabaseMetaData::supportsPositionedDelete),
                new SimpleEntry<>("supportsPositionedUpdate", DatabaseMetaData::supportsPositionedUpdate),
                new SimpleEntry<>("supportsSelectForUpdate", DatabaseMetaData::supportsSelectForUpdate),
                new SimpleEntry<>("supportsStoredProcedures", DatabaseMetaData::supportsStoredProcedures),
                new SimpleEntry<>("supportsSubqueriesInComparisons", DatabaseMetaData::supportsSubqueriesInComparisons),
                new SimpleEntry<>("supportsSubqueriesInExists", DatabaseMetaData::supportsSubqueriesInExists),
                new SimpleEntry<>("supportsSubqueriesInIns", DatabaseMetaData::supportsSubqueriesInIns),
                new SimpleEntry<>("supportsSubqueriesInQuantifieds", DatabaseMetaData::supportsSubqueriesInQuantifieds),
                new SimpleEntry<>("supportsCorrelatedSubqueries", DatabaseMetaData::supportsCorrelatedSubqueries),
                new SimpleEntry<>("supportsUnion", DatabaseMetaData::supportsUnion),
                new SimpleEntry<>("supportsUnionAll", DatabaseMetaData::supportsUnionAll),
                new SimpleEntry<>("supportsOpenCursorsAcrossCommit", DatabaseMetaData::supportsOpenCursorsAcrossCommit),
                new SimpleEntry<>("supportsOpenCursorsAcrossRollback", DatabaseMetaData::supportsOpenCursorsAcrossRollback),
                new SimpleEntry<>("supportsOpenStatementsAcrossCommit", DatabaseMetaData::supportsOpenStatementsAcrossCommit),
                new SimpleEntry<>("supportsOpenStatementsAcrossRollback", DatabaseMetaData::supportsOpenStatementsAcrossRollback),
                new SimpleEntry<>("getMaxBinaryLiteralLength", DatabaseMetaData::getMaxBinaryLiteralLength),
                new SimpleEntry<>("getMaxCharLiteralLength", DatabaseMetaData::getMaxCharLiteralLength),
                new SimpleEntry<>("getMaxColumnNameLength", DatabaseMetaData::getMaxColumnNameLength),
                new SimpleEntry<>("getMaxColumnsInGroupBy", DatabaseMetaData::getMaxColumnsInGroupBy),
                new SimpleEntry<>("getMaxColumnsInIndex", DatabaseMetaData::getMaxColumnsInIndex),
                new SimpleEntry<>("getMaxColumnsInOrderBy", DatabaseMetaData::getMaxColumnsInOrderBy),
                new SimpleEntry<>("getMaxColumnsInSelect", DatabaseMetaData::getMaxColumnsInSelect),
                new SimpleEntry<>("getMaxColumnsInTable", DatabaseMetaData::getMaxColumnsInTable),
                new SimpleEntry<>("getMaxConnections", DatabaseMetaData::getMaxConnections),
                new SimpleEntry<>("getMaxCursorNameLength", DatabaseMetaData::getMaxCursorNameLength),
                new SimpleEntry<>("getMaxIndexLength", DatabaseMetaData::getMaxIndexLength),
                new SimpleEntry<>("getMaxSchemaNameLength", DatabaseMetaData::getMaxSchemaNameLength),
                new SimpleEntry<>("getMaxProcedureNameLength", DatabaseMetaData::getMaxProcedureNameLength),
                new SimpleEntry<>("getMaxCatalogNameLength", DatabaseMetaData::getMaxCatalogNameLength),
                new SimpleEntry<>("getMaxRowSize", DatabaseMetaData::getMaxRowSize),
                new SimpleEntry<>("doesMaxRowSizeIncludeBlobs", DatabaseMetaData::doesMaxRowSizeIncludeBlobs),
                new SimpleEntry<>("getMaxStatementLength", DatabaseMetaData::getMaxStatementLength),
                new SimpleEntry<>("getMaxStatements", DatabaseMetaData::getMaxStatements),
                new SimpleEntry<>("getMaxTableNameLength", DatabaseMetaData::getMaxTableNameLength),
                new SimpleEntry<>("getMaxTablesInSelect", DatabaseMetaData::getMaxTablesInSelect),
                new SimpleEntry<>("getMaxUserNameLength", DatabaseMetaData::getMaxUserNameLength),
                new SimpleEntry<>("getDefaultTransactionIsolation", DatabaseMetaData::getDefaultTransactionIsolation),
                new SimpleEntry<>("supportsTransactions", DatabaseMetaData::supportsTransactions),
                new SimpleEntry<>("supportsBatchUpdates", DatabaseMetaData::supportsBatchUpdates),
                new SimpleEntry<>("supportsSavepoints", DatabaseMetaData::supportsSavepoints),
                new SimpleEntry<>("supportsNamedParameters", DatabaseMetaData::supportsNamedParameters),
                new SimpleEntry<>("supportsMultipleOpenResults", DatabaseMetaData::supportsMultipleOpenResults),
                new SimpleEntry<>("supportsGetGeneratedKeys", DatabaseMetaData::supportsGetGeneratedKeys),
                new SimpleEntry<>("getResultSetHoldability", DatabaseMetaData::getResultSetHoldability),
                new SimpleEntry<>("getDatabaseMajorVersion", DatabaseMetaData::getDatabaseMajorVersion),
                new SimpleEntry<>("getDatabaseMinorVersion", DatabaseMetaData::getDatabaseMinorVersion),
                new SimpleEntry<>("getJDBCMajorVersion", DatabaseMetaData::getJDBCMajorVersion),
                new SimpleEntry<>("getJDBCMinorVersion", DatabaseMetaData::getJDBCMinorVersion),
                new SimpleEntry<>("getSQLStateType", DatabaseMetaData::getSQLStateType),
                new SimpleEntry<>("locatorsUpdateCopy", DatabaseMetaData::locatorsUpdateCopy),
                new SimpleEntry<>("supportsStatementPooling", DatabaseMetaData::supportsStatementPooling),
                new SimpleEntry<>("getRowIdLifetime", DatabaseMetaData::getRowIdLifetime),
                new SimpleEntry<>("supportsStoredFunctionsUsingCallSyntax", DatabaseMetaData::supportsStoredFunctionsUsingCallSyntax),
                new SimpleEntry<>("autoCommitFailureClosesAllResultSets", DatabaseMetaData::autoCommitFailureClosesAllResultSets),
                new SimpleEntry<>("generatedKeyAlwaysReturned", DatabaseMetaData::generatedKeyAlwaysReturned),
                new SimpleEntry<>("getMaxLogicalLobSize", DatabaseMetaData::getMaxLogicalLobSize),
                new SimpleEntry<>("supportsRefCursors", DatabaseMetaData::supportsRefCursors),
                new SimpleEntry<>("supportsSharding", DatabaseMetaData::supportsSharding)
        );

        for (Entry<String, ThrowingFunction<DatabaseMetaData, ?, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingFunction<DatabaseMetaData, ?, SQLException> f = getter.getValue();
            //assertEquals(f.apply(nativeMd), f.apply(httpMd), name);
            assertCall(f, nativeMd, httpMd, name);
        }

        assertSame(httpConn, httpMd.getConnection());
        assertSame(nativeConn, nativeMd.getConnection());
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void resultSetGetters(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        DatabaseMetaData httpMd = httpConn.getMetaData();
        DatabaseMetaData nativeMd = nativeConn.getMetaData();

        Collection<Entry<String, ThrowingFunction<DatabaseMetaData, ResultSet, SQLException>>> getters = Arrays.asList(
                new AbstractMap.SimpleEntry<>("getSchemas", DatabaseMetaData::getSchemas),
                new AbstractMap.SimpleEntry<>("getCatalogs", DatabaseMetaData::getCatalogs),
                new AbstractMap.SimpleEntry<>("getTableTypes", DatabaseMetaData::getTableTypes),
                new AbstractMap.SimpleEntry<>("getTypeInfo", DatabaseMetaData::getTypeInfo)
        );

        for (Entry<String, ThrowingFunction<DatabaseMetaData, ResultSet, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingFunction<DatabaseMetaData, ResultSet, SQLException> f = getter.getValue();
            assertResultSets(nativeMd, httpMd, f, name, Integer.MAX_VALUE);
        }
    }


    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void resultSetGetters2(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        DatabaseMetaData httpMd = httpConn.getMetaData();
        DatabaseMetaData nativeMd = nativeConn.getMetaData();

        Collection<Entry<String, ThrowingTriFunction<DatabaseMetaData, String, String, ResultSet, SQLException>>> getters = Arrays.asList(
                new AbstractMap.SimpleEntry<>("getSchemas", DatabaseMetaData::getSchemas)
        );

        for (Entry<String, ThrowingTriFunction<DatabaseMetaData, String, String, ResultSet, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingTriFunction<DatabaseMetaData, String, String, ResultSet, SQLException> f = getter.getValue();
            assertResultSets(nativeMd, httpMd, md -> f.apply(md, null, null), name, Integer.MAX_VALUE);
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void resultSetGetters3(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        DatabaseMetaData httpMd = httpConn.getMetaData();
        DatabaseMetaData nativeMd = nativeConn.getMetaData();

        Collection<Entry<String, ThrowingQuadraFunction<DatabaseMetaData, String, String, String, ResultSet, SQLException>>> getters = Arrays.asList(
                new AbstractMap.SimpleEntry<>("getProcedures", DatabaseMetaData::getProcedures),
                new AbstractMap.SimpleEntry<>("getTablePrivileges", DatabaseMetaData::getTablePrivileges),
                new AbstractMap.SimpleEntry<>("getVersionColumns", DatabaseMetaData::getVersionColumns),
                new AbstractMap.SimpleEntry<>("getPrimaryKeys", DatabaseMetaData::getPrimaryKeys),
                new AbstractMap.SimpleEntry<>("getImportedKeys", DatabaseMetaData::getImportedKeys),
                new AbstractMap.SimpleEntry<>("getExportedKeys", DatabaseMetaData::getExportedKeys),
                new AbstractMap.SimpleEntry<>("getSuperTypes", DatabaseMetaData::getSuperTypes),
                new AbstractMap.SimpleEntry<>("getSuperTables", DatabaseMetaData::getSuperTables),
                new AbstractMap.SimpleEntry<>("getFunctions", DatabaseMetaData::getFunctions)
        );

        for (Entry<String, ThrowingQuadraFunction<DatabaseMetaData, String, String, String, ResultSet, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingQuadraFunction<DatabaseMetaData, String, String, String, ResultSet, SQLException> f = getter.getValue();
            assertResultSets(nativeMd, httpMd, md -> f.apply(md, null, null, null), name, Integer.MAX_VALUE);
        }
    }

    @ParameterizedTest(name = ARGUMENTS_PLACEHOLDER)
    @JdbcUrls
    void resultSetGetters4(String nativeUrl) throws SQLException {
        Connection httpConn = DriverManager.getConnection(format("%s#%s", httpUrl, nativeUrl));
        Connection nativeConn = DriverManager.getConnection(nativeUrl);

        DatabaseMetaData httpMd = httpConn.getMetaData();
        DatabaseMetaData nativeMd = nativeConn.getMetaData();

        Collection<Entry<String, ThrowingFunction<DatabaseMetaData, ResultSet, SQLException>>> getters = Arrays.asList(
                new AbstractMap.SimpleEntry<>("getFunctionColumns", md -> md.getFunctionColumns(null, null, null, null)),
                new AbstractMap.SimpleEntry<>("getAttributes", md -> md.getAttributes(null, null, null, null)),
                new AbstractMap.SimpleEntry<>("getPseudoColumns", md -> md.getPseudoColumns(null, null, null, null)),
                new AbstractMap.SimpleEntry<>("getProcedureColumns", md -> md.getProcedureColumns(null, null, null, null)),
                new AbstractMap.SimpleEntry<>("getColumns", md -> md.getColumns(null, null, null, null)),
                new AbstractMap.SimpleEntry<>("getColumnPrivileges", md -> md.getColumnPrivileges(null, null, null, null)),

                new AbstractMap.SimpleEntry<>("getTables", md -> md.getTables(null, null, null, null)),
                new AbstractMap.SimpleEntry<>("getTables", md -> md.getTables(null, null, null, new String[0])),
                new AbstractMap.SimpleEntry<>("getUDTs", md -> md.getUDTs(null, null, null, null)),
                new AbstractMap.SimpleEntry<>("getUDTs", md -> md.getUDTs(null, null, null, new int[0])),

                new AbstractMap.SimpleEntry<>("getBestRowIdentifier", md -> md.getBestRowIdentifier(null, null, null, 0, false)),
                new AbstractMap.SimpleEntry<>("getBestRowIdentifier", md -> md.getBestRowIdentifier(null, null, null, 0, true)),

                new AbstractMap.SimpleEntry<>("getIndexInfo", md -> md.getIndexInfo(null, null, null, false, false)),
                new AbstractMap.SimpleEntry<>("getIndexInfo", md -> md.getIndexInfo(null, null, null, false, true)),
                new AbstractMap.SimpleEntry<>("getIndexInfo", md -> md.getIndexInfo(null, null, null, true, false)),
                new AbstractMap.SimpleEntry<>("getIndexInfo", md -> md.getIndexInfo(null, null, null, true, true)),

                new AbstractMap.SimpleEntry<>("getCrossReference", md -> md.getCrossReference(null, null, null, null, null, null))


        );

        for (Entry<String, ThrowingFunction<DatabaseMetaData, ResultSet, SQLException>> getter : getters) {
            String name = getter.getKey();
            ThrowingFunction<DatabaseMetaData, ResultSet, SQLException> f = getter.getValue();
            assertResultSets(nativeMd, httpMd, f, name, 100);
        }
    }


    private <T> void assertResultSets(T obj1, T obj2, ThrowingFunction<T, ResultSet, SQLException> f, String message, int limit) throws SQLException {
        ResultSet nativeRes = null;
        ResultSet httpRes = null;
        Exception nativeEx = null;
        Exception httpEx = null;
        try {
            nativeRes = f.apply(obj1);
        } catch (Exception e) {
            nativeEx = e;
        }
        try {
            httpRes = f.apply(obj2);
        } catch (Exception e) {
            httpEx = e;
        }
        if (nativeEx == null) {
            AssertUtils.assertResultSet(nativeRes, httpRes, message, limit);
        } else {
            assertNotNull(httpEx);
            assertEquals(nativeEx.getMessage(), httpEx.getMessage());
        }
    }
}

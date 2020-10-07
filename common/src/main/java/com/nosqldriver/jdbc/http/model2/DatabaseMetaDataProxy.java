package com.nosqldriver.jdbc.http.model2;

import com.nosqldriver.jdbc.http.HttpConnector;
import com.nosqldriver.jdbc.http.model.ResultSetProxy;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;

import static com.nosqldriver.jdbc.http.Util.pathParameter;
import static java.lang.String.format;

public class DatabaseMetaDataProxy implements DatabaseMetaData {
    protected final String baseUrl;
    protected final HttpConnector connector = new HttpConnector();

    public DatabaseMetaDataProxy(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return connector.get(format("%s/all/procedures/callable", baseUrl), Boolean.class);
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return connector.get(format("%s/all/tables/selectable", baseUrl), Boolean.class);
    }

    @Override
    public String getURL() throws SQLException {
        return connector.get(format("%s/url", baseUrl), String.class);
    }

    @Override
    public String getUserName() throws SQLException {
        return connector.get(format("%s/username", baseUrl), String.class);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connector.get(format("%s/readonly", baseUrl), Boolean.class);
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return connector.get(format("%s/null/sorted/high", baseUrl), Boolean.class);
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return connector.get(format("%s/null/sorted/low", baseUrl), Boolean.class);
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return connector.get(format("%s/null/sorted/start", baseUrl), Boolean.class);
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return connector.get(format("%s/null/sorted/end", baseUrl), Boolean.class);
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return connector.get(format("%s/database/product/name", baseUrl), String.class);
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return connector.get(format("%s/database/product/version", baseUrl), String.class);
    }

    @Override
    public String getDriverName() throws SQLException {
        return connector.get(format("%s/driver/name", baseUrl), String.class);
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return connector.get(format("%s/driver/version", baseUrl), String.class);
    }

    @Override
    public int getDriverMajorVersion() {
        return connector.get(format("%s/driver/version/major", baseUrl), Integer.class);
    }

    @Override
    public int getDriverMinorVersion() {
        return connector.get(format("%s/driver/version/minor", baseUrl), Integer.class);
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return connector.get(format("%s/useslocalfiles", baseUrl), Boolean.class);
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return connector.get(format("%s/useslocalfiles/pertable", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return connector.get(format("%s/supports/caseidentifiers/mixed", baseUrl), Boolean.class);
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return connector.get(format("%s/stores/caseidentifiers/upper", baseUrl), Boolean.class);
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return connector.get(format("%s/stores/caseidentifiers/lower", baseUrl), Boolean.class);
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return connector.get(format("%s/stores/caseidentifiers/mixed", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return connector.get(format("%s/stores/casequotedidentifiers/mixed", baseUrl), Boolean.class);
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return connector.get(format("%s/stores/casequotedidentifiers/upper", baseUrl), Boolean.class);
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return connector.get(format("%s/stores/casequotedidentifiers/lower", baseUrl), Boolean.class);
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return connector.get(format("%s/stores/casequotedidentifiers/mixed", baseUrl), Boolean.class);
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return connector.get(format("%s/identifierquotestring", baseUrl), String.class);
    }

    @Override
    public String getSQLKeywords() throws SQLException {
        return connector.get(format("%s/sql/keywords", baseUrl), String.class);
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return connector.get(format("%s/functions/numeric", baseUrl), String.class);
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return connector.get(format("%s/functions/string", baseUrl), String.class);
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return connector.get(format("%s/functions/system", baseUrl), String.class);
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return connector.get(format("%s/functions/timedate", baseUrl), String.class);
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return connector.get(format("%s/searchstringescape", baseUrl), String.class);
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return connector.get(format("%s/extranamecharacters", baseUrl), String.class);
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return connector.get(format("%s/supports/alter/table/with/addcolumn", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return connector.get(format("%s/supports/alter/table/with/dropcolumn", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return connector.get(format("%s/supports/column/aliases", baseUrl), Boolean.class);
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return connector.get(format("%s/supports/convert", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return connector.get(format("%s/supports/convert?from=%d&to=%d", baseUrl, fromType, toType), Boolean.class);
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return connector.get(format("%s/supports/tablecorrelationnames", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return connector.get(format("%s/supports/differenttablecorrelationnames", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return connector.get(format("%s/supports/orderby/expressions", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return connector.get(format("%s/supports/orderby/unrelated", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return connector.get(format("%s/supports/groupby", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return connector.get(format("%s/supports/groupby/unrelated", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return connector.get(format("%s/supports/groupby/beyondselect", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return connector.get(format("%s/supports/likeescapeclause", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return connector.get(format("%s/supports/multiple/resultsets", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return connector.get(format("%s/supports/multiple/transactions", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return false;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return null;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return null;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return null;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return false;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return connector.get(format("%s/supports/stored/procedures", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return connector.get(format("%s/supports/subqueries/in/compariosons", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return connector.get(format("%s/supports/subqueries/in/exists", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return connector.get(format("%s/supports/subqueries/in/ins", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return connector.get(format("%s/supports/subqueries/in/quantifieds", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return connector.get(format("%s/supports/subqueries/correlated", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return connector.get(format("%s/supports/union", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return connector.get(format("%s/supports/unionall", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return connector.get(format("%s/supports/open/cursors/across/commit", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return connector.get(format("%s/supports/open/cursors/across/rollback", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return connector.get(format("%s/supports/open/statements/across/commit", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return connector.get(format("%s/supports/open/statements/across/rollback", baseUrl), Boolean.class);
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return connector.get(format("%s/max/binaryliterallength", baseUrl), Integer.class);
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return connector.get(format("%s/max/charliterallength", baseUrl), Integer.class);
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return connector.get(format("%s/max/columnnamelength", baseUrl), Integer.class);
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return connector.get(format("%s/max/columns/in/groupby", baseUrl), Integer.class);
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return connector.get(format("%s/max/columns/in/index", baseUrl), Integer.class);
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return connector.get(format("%s/max/columns/in/orderby", baseUrl), Integer.class);
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return connector.get(format("%s/max/columns/in/select", baseUrl), Integer.class);
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return connector.get(format("%s/max/columns/in/table", baseUrl), Integer.class);
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return connector.get(format("%s/max/connections", baseUrl), Integer.class);
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return connector.get(format("%s/max/cursornamelength", baseUrl), Integer.class);
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return 0;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCatalogs() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTableTypes() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        return null;
    }

    @Override
    public ResultSet getTypeInfo() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        return null;
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return false;
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        return null;
    }

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return false;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return connector.get(format("%s/supports/multiple/openresults", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return connector.get(format("%s/supports/getgeneratedkeys", baseUrl), Boolean.class);
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        return connector.get(format("%s/supertypes?catalog=%s&schema=%s&type=%s", baseUrl, catalog, schemaPattern, typeNamePattern), ResultSetProxy.class);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        return connector.get(format("%s/supertables?catalog=%s&schema=%s&table=%s", baseUrl, catalog, schemaPattern, tableNamePattern), ResultSetProxy.class);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        return connector.get(format("%s/attributes?catalog=%s&schema=%s&type=%s&attribute=%s", baseUrl, catalog, schemaPattern, typeNamePattern, attributeNamePattern), ResultSetProxy.class);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return connector.get(format("%s/supports/resultsetholdability/%d", baseUrl, holdability), Boolean.class);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return connector.get(format("%s/resultsetholdability", baseUrl), Integer.class);
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return connector.get(format("%s/database/version/major", baseUrl), Integer.class);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return connector.get(format("%s/database/version/minor", baseUrl), Integer.class);
    }

    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return connector.get(format("%s/jdbc/version/major", baseUrl), Integer.class);
    }

    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return connector.get(format("%s/jdbc/version/minor", baseUrl), Integer.class);
    }

    @Override
    public int getSQLStateType() throws SQLException {
        return connector.get(format("%s/sql/statetype", baseUrl), Integer.class);
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return connector.get(format("%s/locatorsupdatecopy", baseUrl), Boolean.class);
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return connector.get(format("%s/supports/statementpooling", baseUrl), Boolean.class);
    }

    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return connector.get(format("%s/rowidlifetime", baseUrl), RowIdLifetime.class);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        return connector.get(format("%s/schemas?catalog=%s&schema=%s", baseUrl, catalog, schemaPattern), ResultSetProxy.class);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return connector.get(format("%s/supports/stored/functionsusingcallsyntax", baseUrl), Boolean.class);
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        return null;
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/functions", baseUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"function", functionNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/function/columns", baseUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"function", functionNamePattern}, new String[] {"column", columnNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        return null;
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return (T)connector.get(format("%s/unwrap%s", baseUrl, pathParameter(iface)), Object.class);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return connector.get(format("%s/wrapper%s", baseUrl, pathParameter(iface)), Boolean.class);
    }
}

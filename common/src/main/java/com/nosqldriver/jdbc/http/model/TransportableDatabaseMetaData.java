package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class TransportableDatabaseMetaData extends WrapperProxy implements DatabaseMetaData {
    @JsonProperty private boolean allProceduresAreCallable;
    @JsonProperty private boolean allTablesAreSelectable;
    @JsonProperty private String url;
    @JsonProperty private String userName;
    @JsonProperty private boolean readOnly;
    @JsonProperty private boolean nullsAreSortedHigh;
    @JsonProperty private boolean nullsAreSortedLow;
    @JsonProperty private boolean nullsAreSortedAtStart;
    @JsonProperty private boolean nullsAreSortedAtEnd;
    @JsonProperty private String databaseProductName;
    @JsonProperty private String databaseProductVersion;
    @JsonProperty private String driverName;
    @JsonProperty private String driverVersion;
    @JsonProperty private int driverMajorVersion;
    @JsonProperty private int driverMinorVersion;
    @JsonProperty private boolean usesLocalFiles;
    @JsonProperty private boolean usesLocalFilePerTable;
    @JsonProperty private boolean supportsMixedCaseIdentifiers;
    @JsonProperty private boolean storesUpperCaseIdentifiers;
    @JsonProperty private boolean storesLowerCaseIdentifiers;
    @JsonProperty private boolean storesMixedCaseIdentifiers;
    @JsonProperty private boolean supportsMixedCaseQuotedIdentifiers;
    @JsonProperty private boolean storesUpperCaseQuotedIdentifiers;
    @JsonProperty private boolean storesLowerCaseQuotedIdentifiers;
    @JsonProperty private boolean storesMixedCaseQuotedIdentifiers;
    @JsonProperty private String identifierQuoteString;
    @JsonProperty("sqlKeywords")
    private String sqlKeywords;
    @JsonProperty private String numericFunctions;
    @JsonProperty private String stringFunctions;
    @JsonProperty private String systemFunctions;
    @JsonProperty private String timeDateFunctions;
    @JsonProperty private String searchStringEscape;
    @JsonProperty private String extraNameCharacters;
    @JsonProperty private boolean supportsAlterTableWithAddColumn;
    @JsonProperty private boolean supportsAlterTableWithDropColumn;
    @JsonProperty private boolean supportsColumnAliasing;
    @JsonProperty private boolean nullPlusNonNullIsNull;
    @JsonProperty private boolean supportsConvert;
    @JsonProperty private boolean supportsTableCorrelationNames;
    @JsonProperty private boolean supportsDifferentTableCorrelationNames;
    @JsonProperty private boolean supportsExpressionsInOrderBy;
    @JsonProperty private boolean supportsOrderByUnrelated;
    @JsonProperty private boolean supportsGroupBy;
    @JsonProperty private boolean supportsGroupByUnrelated;
    @JsonProperty private boolean supportsGroupByBeyondSelect;
    @JsonProperty private boolean supportsLikeEscapeClause;
    @JsonProperty private boolean supportsMultipleResultSets;
    @JsonProperty private boolean supportsMultipleTransactions;
    @JsonProperty private boolean supportsNonNullableColumns;
    @JsonProperty private boolean supportsMinimumSQLGrammar;
    @JsonProperty private boolean supportsCoreSQLGrammar;
    @JsonProperty private boolean supportsExtendedSQLGrammar;
    @JsonProperty private boolean supportsANSI92EntryLevelSQL;
    @JsonProperty private boolean supportsANSI92IntermediateSQL;
    @JsonProperty private boolean supportsANSI92FullSQL;
    @JsonProperty private boolean supportsIntegrityEnhancementFacility;
    @JsonProperty private boolean supportsOuterJoins;
    @JsonProperty private boolean supportsFullOuterJoins;
    @JsonProperty private boolean supportsLimitedOuterJoins;
    @JsonProperty private String schemaTerm;
    @JsonProperty private String procedureTerm;
    @JsonProperty private String catalogTerm;
    @JsonProperty private boolean catalogAtStart;
    @JsonProperty private String catalogSeparator;
    @JsonProperty private boolean supportsSchemasInDataManipulation;
    @JsonProperty private boolean supportsSchemasInProcedureCalls;
    @JsonProperty private boolean supportsSchemasInTableDefinitions;
    @JsonProperty private boolean supportsSchemasInIndexDefinitions;
    @JsonProperty private boolean supportsSchemasInPrivilegeDefinitions;
    @JsonProperty private boolean supportsCatalogsInDataManipulation;
    @JsonProperty private boolean supportsCatalogsInProcedureCalls;
    @JsonProperty private boolean supportsCatalogsInTableDefinitions;
    @JsonProperty private boolean supportsCatalogsInIndexDefinitions;
    @JsonProperty private boolean supportsCatalogsInPrivilegeDefinitions;
    @JsonProperty private boolean supportsPositionedDelete;
    @JsonProperty private boolean supportsPositionedUpdate;
    @JsonProperty private boolean supportsSelectForUpdate;
    @JsonProperty private boolean supportsStoredProcedures;
    @JsonProperty private boolean supportsSubqueriesInComparisons;
    @JsonProperty private boolean supportsSubqueriesInExists;
    @JsonProperty private boolean supportsSubqueriesInIns;
    @JsonProperty private boolean supportsSubqueriesInQuantifieds;
    @JsonProperty private boolean supportsCorrelatedSubqueries;
    @JsonProperty private boolean supportsUnion;
    @JsonProperty private boolean supportsUnionAll;
    @JsonProperty private boolean supportsOpenCursorsAcrossCommit;
    @JsonProperty private boolean supportsOpenCursorsAcrossRollback;
    @JsonProperty private boolean supportsOpenStatementsAcrossCommit;
    @JsonProperty private boolean supportsOpenStatementsAcrossRollback;
    @JsonProperty private int maxBinaryLiteralLength;
    @JsonProperty private int maxCharLiteralLength;
    @JsonProperty private int maxColumnNameLength;
    @JsonProperty private int maxColumnsInGroupBy;
    @JsonProperty private int maxColumnsInIndex;
    @JsonProperty private int maxColumnsInOrderBy;
    @JsonProperty private int maxColumnsInSelect;
    @JsonProperty private int maxColumnsInTable;
    @JsonProperty private int maxConnections;
    @JsonProperty private int maxCursorNameLength;
    @JsonProperty private int maxIndexLength;
    @JsonProperty private int maxSchemaNameLength;
    @JsonProperty private int maxProcedureNameLength;
    @JsonProperty private int maxCatalogNameLength;
    @JsonProperty private int maxRowSize;
    @JsonProperty private boolean doesMaxRowSizeIncludeBlobs;
    @JsonProperty private int maxStatementLength;
    @JsonProperty private int maxStatements;
    @JsonProperty private int maxTableNameLength;
    @JsonProperty private int maxTablesInSelect;
    @JsonProperty private int maxUserNameLength;
    @JsonProperty private int defaultTransactionIsolation;
    @JsonProperty private boolean supportsTransactions;
    @JsonProperty private boolean supportsDataDefinitionAndDataManipulationTransactions;
    @JsonProperty private boolean supportsDataManipulationTransactionsOnly;
    @JsonProperty private boolean dataDefinitionCausesTransactionCommit;
    @JsonProperty private boolean dataDefinitionIgnoredInTransactions;
    @JsonProperty private boolean supportsBatchUpdates;
    @JsonProperty private Connection connection;
    @JsonProperty private boolean supportsSavepoints;
    @JsonProperty private boolean supportsNamedParameters;
    @JsonProperty private boolean supportsMultipleOpenResults;
    @JsonProperty private boolean supportsGetGeneratedKeys;
    @JsonProperty private int resultSetHoldability;
    @JsonProperty private int databaseMajorVersion;
    @JsonProperty private int databaseMinorVersion;
    @JsonProperty("jdbcMajorVersion")
    private int jdbcMajorVersion;
    @JsonProperty("jdbcMinorVersion")
    private int jdbcMinorVersion;
    @JsonProperty("sqlStateType")
    private int sqlStateType;
    @JsonProperty private boolean locatorsUpdateCopy;
    @JsonProperty private boolean supportsStatementPooling;
//    @JsonProperty private RowIdLifetime rowIdLifetime;
    @JsonProperty private boolean supportsStoredFunctionsUsingCallSyntax;
    @JsonProperty private boolean autoCommitFailureClosesAllResultSets;
    @JsonProperty private boolean  generatedKeyAlwaysReturned;
    @JsonProperty private long maxLogicalLobSize;
    @JsonProperty private boolean supportsRefCursors;
    @JsonProperty private boolean supportsSharding;

    @JsonCreator
    public TransportableDatabaseMetaData(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl, DatabaseMetaData.class);
    }

    public TransportableDatabaseMetaData(String entityUrl, DatabaseMetaData md) throws SQLException {
        super(entityUrl, DatabaseMetaData.class);
        this.allProceduresAreCallable = md.allProceduresAreCallable();
        this.allTablesAreSelectable = md.allTablesAreSelectable();
        this.url = md.getURL();
        this.userName = md.getUserName();
        this.readOnly = md.isReadOnly();
        this.nullsAreSortedHigh = md.nullsAreSortedHigh();
        this.nullsAreSortedLow = md.nullsAreSortedLow();
        this.nullsAreSortedAtStart = md.nullsAreSortedAtStart();
        this.nullsAreSortedAtEnd = md.nullsAreSortedAtEnd();
        this.databaseProductName = md.getDatabaseProductName();
        this.databaseProductVersion = md.getDatabaseProductVersion();
        this.driverName = md.getDriverName();
        this.driverVersion = md.getDriverVersion();
        this.driverMajorVersion = md.getDriverMajorVersion();
        this.driverMinorVersion = md.getDriverMinorVersion();
        this.usesLocalFiles = md.usesLocalFiles();
        this.usesLocalFilePerTable = md.usesLocalFilePerTable();
        this.supportsMixedCaseIdentifiers = md.supportsMixedCaseIdentifiers();
        this.storesUpperCaseIdentifiers = md.storesUpperCaseIdentifiers();
        this.storesLowerCaseIdentifiers = md.storesLowerCaseIdentifiers();
        this.storesMixedCaseIdentifiers = md.storesMixedCaseIdentifiers();
        this.supportsMixedCaseQuotedIdentifiers = md.supportsMixedCaseQuotedIdentifiers();
        this.storesUpperCaseQuotedIdentifiers = md.storesUpperCaseQuotedIdentifiers();
        this.storesLowerCaseQuotedIdentifiers = md.storesLowerCaseQuotedIdentifiers();
        this.storesMixedCaseQuotedIdentifiers = md.storesMixedCaseQuotedIdentifiers();
        this.identifierQuoteString = md.getIdentifierQuoteString();
        this.sqlKeywords = md.getSQLKeywords();
        this.numericFunctions = md.getNumericFunctions();
        this.stringFunctions = md.getStringFunctions();
        this.systemFunctions = md.getSystemFunctions();
        this.timeDateFunctions = md.getTimeDateFunctions();
        this.searchStringEscape = md.getSearchStringEscape();
        this.extraNameCharacters = md.getExtraNameCharacters();
        this.supportsAlterTableWithAddColumn = md.supportsAlterTableWithAddColumn();
        this.supportsAlterTableWithDropColumn = md.supportsAlterTableWithDropColumn();
        this.supportsColumnAliasing = md.supportsColumnAliasing();
        this.nullPlusNonNullIsNull = md.nullPlusNonNullIsNull();
        this.supportsConvert = md.supportsConvert();
        this.supportsTableCorrelationNames = md.supportsTableCorrelationNames();
        this.supportsDifferentTableCorrelationNames = md.supportsDifferentTableCorrelationNames();
        this.supportsExpressionsInOrderBy = md.supportsExpressionsInOrderBy();
        this.supportsOrderByUnrelated = md.supportsOrderByUnrelated();
        this.supportsGroupBy = md.supportsGroupBy();
        this.supportsGroupByUnrelated = md.supportsGroupByUnrelated();
        this.supportsGroupByBeyondSelect = md.supportsGroupByBeyondSelect();
        this.supportsLikeEscapeClause = md.supportsLikeEscapeClause();
        this.supportsMultipleResultSets = md.supportsMultipleResultSets();
        this.supportsMultipleTransactions = md.supportsMultipleTransactions();
        this.supportsNonNullableColumns = md.supportsNonNullableColumns();
        this.supportsMinimumSQLGrammar = md.supportsMinimumSQLGrammar();
        this.supportsCoreSQLGrammar = md.supportsCoreSQLGrammar();
        this.supportsExtendedSQLGrammar = md.supportsExtendedSQLGrammar();
        this.supportsANSI92EntryLevelSQL = md.supportsANSI92EntryLevelSQL();
        this.supportsANSI92IntermediateSQL = md.supportsANSI92IntermediateSQL();
        this.supportsANSI92FullSQL = md.supportsANSI92FullSQL();
        this.supportsIntegrityEnhancementFacility = md.supportsIntegrityEnhancementFacility();
        this.supportsOuterJoins = md.supportsOuterJoins();
        this.supportsFullOuterJoins = md.supportsFullOuterJoins();
        this.supportsLimitedOuterJoins = md.supportsLimitedOuterJoins();
        this.schemaTerm = md.getSchemaTerm();
        this.procedureTerm = md.getProcedureTerm();
        this.catalogTerm = md.getCatalogTerm();
        this.catalogAtStart = md.isCatalogAtStart();
        this.catalogSeparator = md.getCatalogSeparator();
        this.supportsSchemasInDataManipulation = md.supportsSchemasInDataManipulation();
        this.supportsSchemasInProcedureCalls = md.supportsSchemasInProcedureCalls();
        this.supportsSchemasInTableDefinitions = md.supportsSchemasInTableDefinitions();
        this.supportsSchemasInIndexDefinitions = md.supportsSchemasInIndexDefinitions();
        this.supportsSchemasInPrivilegeDefinitions = md.supportsSchemasInPrivilegeDefinitions();
        this.supportsCatalogsInDataManipulation = md.supportsCatalogsInDataManipulation();
        this.supportsCatalogsInProcedureCalls = md.supportsCatalogsInProcedureCalls();
        this.supportsCatalogsInTableDefinitions = md.supportsCatalogsInTableDefinitions();
        this.supportsCatalogsInIndexDefinitions = md.supportsCatalogsInIndexDefinitions();
        this.supportsCatalogsInPrivilegeDefinitions = md.supportsCatalogsInPrivilegeDefinitions();
        this.supportsPositionedDelete = md.supportsPositionedDelete();
        this.supportsPositionedUpdate = md.supportsPositionedUpdate();
        this.supportsSelectForUpdate = md.supportsSelectForUpdate();
        this.supportsStoredProcedures = md.supportsStoredProcedures();
        this.supportsSubqueriesInComparisons = md.supportsSubqueriesInComparisons();
        this.supportsSubqueriesInExists = md.supportsSubqueriesInExists();
        this.supportsSubqueriesInIns = md.supportsSubqueriesInIns();
        this.supportsSubqueriesInQuantifieds = md.supportsSubqueriesInQuantifieds();
        this.supportsCorrelatedSubqueries = md.supportsCorrelatedSubqueries();
        this.supportsUnion = md.supportsUnion();
        this.supportsUnionAll = md.supportsUnionAll();
        this.supportsOpenCursorsAcrossCommit = md.supportsOpenCursorsAcrossCommit();
        this.supportsOpenCursorsAcrossRollback = md.supportsOpenCursorsAcrossRollback();
        this.supportsOpenStatementsAcrossCommit = md.supportsOpenStatementsAcrossCommit();
        this.supportsOpenStatementsAcrossRollback = md.supportsOpenStatementsAcrossRollback();
        this.maxBinaryLiteralLength = md.getMaxBinaryLiteralLength();
        this.maxCharLiteralLength = md.getMaxCharLiteralLength();
        this.maxColumnNameLength = md.getMaxColumnNameLength();
        this.maxColumnsInGroupBy = md.getMaxColumnsInGroupBy();
        this.maxColumnsInIndex = md.getMaxColumnsInIndex();
        this.maxColumnsInOrderBy = md.getMaxColumnsInOrderBy();
        this.maxColumnsInSelect = md.getMaxColumnsInSelect();
        this.maxColumnsInTable = md.getMaxColumnsInTable();
        this.maxConnections = md.getMaxConnections();
        this.maxCursorNameLength = md.getMaxCursorNameLength();
        this.maxIndexLength = md.getMaxIndexLength();
        this.maxSchemaNameLength = md.getMaxSchemaNameLength();
        this.maxProcedureNameLength = md.getMaxProcedureNameLength();
        this.maxCatalogNameLength = md.getMaxCatalogNameLength();
        this.maxRowSize = md.getMaxRowSize();
        this.doesMaxRowSizeIncludeBlobs = md.doesMaxRowSizeIncludeBlobs();
        this.maxStatementLength = md.getMaxStatementLength();
        this.maxStatements = md.getMaxStatements();
        this.maxTableNameLength = md.getMaxTableNameLength();
        this.maxTablesInSelect = md.getMaxTablesInSelect();
        this.maxUserNameLength = md.getMaxUserNameLength();
        this.defaultTransactionIsolation = md.getDefaultTransactionIsolation();
        this.supportsTransactions = md.supportsTransactions();
        this.supportsDataDefinitionAndDataManipulationTransactions = md.supportsDataDefinitionAndDataManipulationTransactions();
        this.supportsDataManipulationTransactionsOnly = md.supportsDataManipulationTransactionsOnly();
        this.dataDefinitionCausesTransactionCommit = md.dataDefinitionCausesTransactionCommit();
        this.dataDefinitionIgnoredInTransactions = md.dataDefinitionIgnoredInTransactions();
        this.supportsBatchUpdates = md.supportsBatchUpdates();
        this.supportsSavepoints = md.supportsSavepoints();
        this.supportsNamedParameters = md.supportsNamedParameters();
        this.supportsMultipleOpenResults = md.supportsMultipleOpenResults();
        this.supportsGetGeneratedKeys = md.supportsGetGeneratedKeys();
        this.resultSetHoldability = md.getResultSetHoldability();
        this.databaseMajorVersion = md.getDatabaseMajorVersion();
        this.databaseMinorVersion = md.getDatabaseMinorVersion();
        this.jdbcMajorVersion = md.getJDBCMajorVersion();
        this.jdbcMinorVersion = md.getJDBCMinorVersion();
        this.sqlStateType = md.getSQLStateType();
        this.locatorsUpdateCopy = md.locatorsUpdateCopy();
        this.supportsStatementPooling = md.supportsStatementPooling();
        //this.rowIdLifetime = md.getRowIdLifetime();
        this.supportsStoredFunctionsUsingCallSyntax = md.supportsStoredFunctionsUsingCallSyntax();
        this.autoCommitFailureClosesAllResultSets = md.autoCommitFailureClosesAllResultSets();
        this.generatedKeyAlwaysReturned = md.generatedKeyAlwaysReturned();
        this.maxLogicalLobSize = md.getMaxLogicalLobSize();
        this.supportsRefCursors = md.supportsRefCursors();
        this.supportsSharding = md.supportsSharding();
    }


    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return allProceduresAreCallable;
    }

    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return allTablesAreSelectable;
    }

    @Override
    public String getURL() throws SQLException {
        return url;
    }

    @Override
    public String getUserName() throws SQLException {
        return userName;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return nullsAreSortedHigh;
    }

    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return nullsAreSortedLow;
    }

    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return nullsAreSortedAtStart;
    }

    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return nullsAreSortedAtEnd;
    }

    @Override
    public String getDatabaseProductName() throws SQLException {
        return databaseProductName;
    }

    @Override
    public String getDatabaseProductVersion() throws SQLException {
        return databaseProductVersion;
    }

    @Override
    public String getDriverName() throws SQLException {
        return driverName;
    }

    @Override
    public String getDriverVersion() throws SQLException {
        return driverVersion;
    }

    @Override
    public int getDriverMajorVersion() {
        return driverMajorVersion;
    }

    @Override
    public int getDriverMinorVersion() {
        return driverMinorVersion;
    }

    @Override
    public boolean usesLocalFiles() throws SQLException {
        return usesLocalFiles;
    }

    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return usesLocalFilePerTable;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return supportsMixedCaseIdentifiers;
    }

    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return storesUpperCaseIdentifiers;
    }

    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return storesLowerCaseIdentifiers;
    }

    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return storesMixedCaseIdentifiers;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return supportsMixedCaseQuotedIdentifiers;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return storesUpperCaseQuotedIdentifiers;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return storesLowerCaseQuotedIdentifiers;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return storesMixedCaseQuotedIdentifiers;
    }

    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return identifierQuoteString;
    }

    @Override
    @JsonProperty("sqlKeywords")
    public String getSQLKeywords() throws SQLException {
        return sqlKeywords;
    }

    @Override
    public String getNumericFunctions() throws SQLException {
        return numericFunctions;
    }

    @Override
    public String getStringFunctions() throws SQLException {
        return stringFunctions;
    }

    @Override
    public String getSystemFunctions() throws SQLException {
        return systemFunctions;
    }

    @Override
    public String getTimeDateFunctions() throws SQLException {
        return timeDateFunctions;
    }

    @Override
    public String getSearchStringEscape() throws SQLException {
        return searchStringEscape;
    }

    @Override
    public String getExtraNameCharacters() throws SQLException {
        return extraNameCharacters;
    }

    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return supportsAlterTableWithAddColumn;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return supportsAlterTableWithDropColumn;
    }

    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return supportsColumnAliasing;
    }

    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return nullPlusNonNullIsNull;
    }

    @Override
    public boolean supportsConvert() throws SQLException {
        return supportsConvert;
    }

    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return connector.get(format("%s/supports/convert/%d/%d", super.entityUrl, fromType, toType), Boolean.class);
    }

    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return supportsTableCorrelationNames;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return supportsDifferentTableCorrelationNames;
    }

    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return supportsExpressionsInOrderBy;
    }

    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return supportsOrderByUnrelated;
    }

    @Override
    public boolean supportsGroupBy() throws SQLException {
        return supportsGroupBy;
    }

    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return supportsGroupByUnrelated;
    }

    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return supportsGroupByBeyondSelect;
    }

    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return supportsLikeEscapeClause;
    }

    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return supportsMultipleResultSets;
    }

    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return supportsMultipleTransactions;
    }

    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return supportsNonNullableColumns;
    }

    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return supportsMinimumSQLGrammar;
    }

    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return supportsCoreSQLGrammar;
    }

    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return supportsExtendedSQLGrammar;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return supportsANSI92EntryLevelSQL;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return supportsANSI92IntermediateSQL;
    }

    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return supportsANSI92FullSQL;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return supportsIntegrityEnhancementFacility;
    }

    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return supportsOuterJoins;
    }

    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return supportsFullOuterJoins;
    }

    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return supportsLimitedOuterJoins;
    }

    @Override
    public String getSchemaTerm() throws SQLException {
        return schemaTerm;
    }

    @Override
    public String getProcedureTerm() throws SQLException {
        return procedureTerm;
    }

    @Override
    public String getCatalogTerm() throws SQLException {
        return catalogTerm;
    }

    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return catalogAtStart;
    }

    @Override
    public String getCatalogSeparator() throws SQLException {
        return catalogSeparator;
    }

    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return supportsSchemasInDataManipulation;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return supportsSchemasInProcedureCalls;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return supportsSchemasInTableDefinitions;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return supportsSchemasInIndexDefinitions;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return supportsSchemasInPrivilegeDefinitions;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return supportsCatalogsInDataManipulation;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return supportsCatalogsInProcedureCalls;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return supportsCatalogsInTableDefinitions;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return supportsCatalogsInIndexDefinitions;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return supportsCatalogsInPrivilegeDefinitions;
    }

    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return supportsPositionedDelete;
    }

    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return supportsPositionedUpdate;
    }

    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return supportsSelectForUpdate;
    }

    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return supportsStoredProcedures;
    }

    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return supportsSubqueriesInComparisons;
    }

    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return supportsSubqueriesInExists;
    }

    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return supportsSubqueriesInIns;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return supportsSubqueriesInQuantifieds;
    }

    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return supportsCorrelatedSubqueries;
    }

    @Override
    public boolean supportsUnion() throws SQLException {
        return supportsUnion;
    }

    @Override
    public boolean supportsUnionAll() throws SQLException {
        return supportsUnionAll;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return supportsOpenCursorsAcrossCommit;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return supportsOpenCursorsAcrossRollback;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return supportsOpenStatementsAcrossCommit;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return supportsOpenStatementsAcrossRollback;
    }

    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return maxBinaryLiteralLength;
    }

    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return maxCharLiteralLength;
    }

    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return maxColumnNameLength;
    }

    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return maxColumnsInGroupBy;
    }

    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return maxColumnsInIndex;
    }

    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return maxColumnsInOrderBy;
    }

    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return maxColumnsInSelect;
    }

    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return maxColumnsInTable;
    }

    @Override
    public int getMaxConnections() throws SQLException {
        return maxConnections;
    }

    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return maxCursorNameLength;
    }

    @Override
    public int getMaxIndexLength() throws SQLException {
        return maxIndexLength;
    }

    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return maxSchemaNameLength;
    }

    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return maxProcedureNameLength;
    }

    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return maxCatalogNameLength;
    }

    @Override
    public int getMaxRowSize() throws SQLException {
        return maxRowSize;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return doesMaxRowSizeIncludeBlobs;
    }

    @Override
    public int getMaxStatementLength() throws SQLException {
        return maxStatementLength;
    }

    @Override
    public int getMaxStatements() throws SQLException {
        return maxStatements;
    }

    @Override
    public int getMaxTableNameLength() throws SQLException {
        return maxTableNameLength;
    }

    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return maxTablesInSelect;
    }

    @Override
    public int getMaxUserNameLength() throws SQLException {
        return maxUserNameLength;
    }

    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return defaultTransactionIsolation;
    }

    @Override
    public boolean supportsTransactions() throws SQLException {
        return supportsTransactions;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        return connector.get(format("%s/supports/transaction/isolation/level/%d", super.entityUrl, level), Boolean.class);
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return supportsDataDefinitionAndDataManipulationTransactions;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return supportsDataManipulationTransactionsOnly;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return dataDefinitionCausesTransactionCommit;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return dataDefinitionIgnoredInTransactions;
    }

    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/procedures", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"procedure", procedureNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/procedure/columns", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"procedure", procedureNamePattern}, new String[] {"column", columnNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        String typesStr = types == null ? null : String.join(",", types);
        String fullUrl = connector.buildUrl(format("%s/tables", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"table", tableNamePattern}, new String[] {"types", typesStr});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getSchemas() throws SQLException {
        return connector.get(format("%s/schemas", super.entityUrl), ResultSetProxy.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getCatalogs() throws SQLException {
        return connector.get(format("%s/catalogs", super.entityUrl), ResultSetProxy.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getTableTypes() throws SQLException {
        return connector.get(format("%s/table/types", super.entityUrl), ResultSetProxy.class);
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/columns", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"table", tableNamePattern}, new String[] {"column", columnNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schema, String table, String columnNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/column/privileges", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schema}, new String[] {"table", table}, new String[] {"column", columnNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/table/privileges", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"table", tableNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/best/row/identifier", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schema}, new String[] {"table", table}, new String[] {"scope", "" + scope}, new String[] {"nullable", "" + nullable});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/version/columns", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schema}, new String[] {"table", table});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/primary/keys", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schema}, new String[] {"table", table});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/imported/keys", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schema}, new String[] {"table", table});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/exported/keys", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schema}, new String[] {"table", table});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable, String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/crossreference", super.entityUrl),
                new String[]{"parentCatalog", parentCatalog}, new String[]{"parentSchema", parentSchema}, new String[]{"parentTable", parentTable},
                new String[]{"foreignCatalog", foreignCatalog}, new String[]{"foreignSchema", foreignSchema}, new String[]{"foreignTable", foreignTable}
        );
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getTypeInfo() throws SQLException {
        return connector.get(format("%s/type/info", super.entityUrl), ResultSetProxy.class);
    }

    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/index/info", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schema}, new String[] {"table", table}, new String[] {"unique", "" + unique}, new String[] {"approximate", "" + approximate});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return connector.get(format("%s/supports/resultset/type/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return connector.get(format("%s/supports/resultset/concurrency/%d/%d", super.entityUrl, type, concurrency), Boolean.class);
    }

    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return connector.get(format("%s/own/updates/visible/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return connector.get(format("%s/own/deletes/visible/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return connector.get(format("%s/own/inserts/visible/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return connector.get(format("%s/others/updates/visible/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return connector.get(format("%s/others/deletes/visible/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return connector.get(format("%s/others/inserts/visible/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean updatesAreDetected(int type) throws SQLException {
        return connector.get(format("%s/updates/detected/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean deletesAreDetected(int type) throws SQLException {
        return connector.get(format("%s/deletes/detected/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean insertsAreDetected(int type) throws SQLException {
        return connector.get(format("%s/inserts/detected/%d", super.entityUrl, type), Boolean.class);
    }

    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return supportsBatchUpdates;
    }

    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        String typesStr = types == null ? null : Arrays.stream(types).mapToObj(i -> ""+i).collect(Collectors.joining(","));
        String fullUrl = connector.buildUrl(format("%s/udts", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"typename", typeNamePattern}, new String[] {"types", typesStr});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    @JsonIgnore
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean supportsSavepoints() throws SQLException {
        return supportsSavepoints;
    }

    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return supportsNamedParameters;
    }

    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return supportsMultipleOpenResults;
    }

    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return supportsGetGeneratedKeys;
    }

    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/super/types", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"typename", typeNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/super/tables", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"table", tableNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/attributes", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"typename", schemaPattern}, new String[] {"attribute", attributeNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return connector.get(format("%s/supports/resultset/holdability/%d", super.entityUrl, holdability), Boolean.class);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return resultSetHoldability;
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return databaseMajorVersion;
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return databaseMinorVersion;
    }

    @Override
    @JsonProperty("jdbcMajorVersion")
    public int getJDBCMajorVersion() throws SQLException {
        return jdbcMajorVersion;
    }

    @Override
    @JsonProperty("jdbcMinorVersion")
    public int getJDBCMinorVersion() throws SQLException {
        return jdbcMinorVersion;
    }

    @Override
    @JsonProperty("sqlStateType")
    public int getSQLStateType() throws SQLException {
        return sqlStateType;
    }

    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return locatorsUpdateCopy;
    }

    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return supportsStatementPooling;
    }

    @Override
    @JsonIgnore
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return connector.get(format("%s/rowidlifetime", super.entityUrl), RowIdLifetime.class);
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/schemas", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return supportsStoredFunctionsUsingCallSyntax;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return autoCommitFailureClosesAllResultSets;
    }

    @Override
    @JsonIgnore
    public ResultSet getClientInfoProperties() throws SQLException {
        return connector.get(format("%s/client/info/properties", super.entityUrl), ResultSetProxy.class);
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/functions", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"function", functionNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/function/columns", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"function", functionNamePattern}, new String[] {"column", columnNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        String fullUrl = connector.buildUrl(format("%s/pseudo/columns", super.entityUrl), new String[] {"catalog", catalog}, new String[] {"schema", schemaPattern}, new String[] {"table", tableNamePattern}, new String[] {"column", columnNamePattern});
        return connector.get(fullUrl, ResultSetProxy.class);
    }

    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return generatedKeyAlwaysReturned;
    }

    @Override
    public long getMaxLogicalLobSize() throws SQLException {
        return maxLogicalLobSize;
    }

    @Override
    public boolean supportsRefCursors() throws SQLException {
        return supportsRefCursors;
    }

    @Override
    public boolean supportsSharding() throws SQLException {
        return supportsSharding;
    }

    public TransportableDatabaseMetaData withConnection(Connection connection) {
        this.connection = connection;
        return this;
    }
}

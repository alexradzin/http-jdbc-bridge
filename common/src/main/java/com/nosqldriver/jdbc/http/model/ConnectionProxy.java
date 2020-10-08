package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.Util;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import static com.nosqldriver.jdbc.http.Util.encode;
import static java.lang.String.format;

public class ConnectionProxy extends WrapperProxy implements Connection {
    @JsonCreator
    public ConnectionProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return connector.post(format("%s/statement", entityUrl), null, StatementProxy.class).withConnection(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connector.post(format("%s/prepared-statement", entityUrl), sql, PreparedStatementProxy.class).withConnection(this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return connector.post(format("%s/callable-statement", entityUrl), sql, CallableStatementProxy.class);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return connector.post(format("%s/nativesql", entityUrl), sql, String.class);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        connector.post(format("%s/autocommit", entityUrl), autoCommit, Void.class);
    }

    @Override
    @JsonIgnore
    public boolean getAutoCommit() throws SQLException {
        return connector.get(format("%s/autocommit", entityUrl), Boolean.class);
    }

    @Override
    public void commit() throws SQLException {
        connector.post(format("%s/commit", entityUrl), null, Void.class);
    }

    @Override
    public void rollback() throws SQLException {
        connector.post(format("%s/rollback", entityUrl), null, Void.class);
    }

    @Override
    public void close() throws SQLException {
        //connector.post(format("%s/close", entityUrl), null, Void.class);
        connector.delete(entityUrl, null, Void.class);
    }

    @Override
    @JsonIgnore
    public boolean isClosed() throws SQLException {
        return connector.get(format("%s/closed", entityUrl), Boolean.class);
    }

    @Override
    @JsonIgnore
    public DatabaseMetaData getMetaData() throws SQLException {
        return connector.get(format("%s/metadata", entityUrl), TransportableDatabaseMetaData.class).withConnection(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        connector.post(format("%s/readonly", entityUrl), readOnly, Void.class);
    }

    @Override
    @JsonIgnore
    public boolean isReadOnly() throws SQLException {
        return connector.get(format("%s/readonly", entityUrl), Boolean.class);
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        connector.post(format("%s/catalog", entityUrl), catalog, Void.class);
    }

    @Override
    @JsonIgnore
    public String getCatalog() throws SQLException {
        return connector.get(format("%s/catalog", entityUrl), String.class);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        connector.post(format("%s/transaction/isolation", entityUrl), level, Void.class);
    }

    @Override
    @JsonIgnore
    public int getTransactionIsolation() throws SQLException {
        return connector.get(format("%s/transaction/isolation", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public SQLWarning getWarnings() throws SQLException {
        return connector.get(format("%s/warnings", entityUrl), TransportableSQLWarning.class);
    }

    @Override
    public void clearWarnings() throws SQLException {
        connector.delete(format("%s/warnings", entityUrl), null, Void.class);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return connector.post(format("%s/statement?type=%d&concurrency=%d", entityUrl, resultSetType, resultSetConcurrency), null, StatementProxy.class).withConnection(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connector.post(format("%s/prepared-statement?type=%d&concurrency=%d", entityUrl, resultSetType, resultSetConcurrency), sql, PreparedStatementProxy.class).withConnection(this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connector.post(format("%s/callable-statement?type=%d&concurrency=%d", entityUrl, resultSetType, resultSetConcurrency), sql, CallableStatementProxy.class);
    }

    @Override
    @JsonIgnore
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        //noinspection unchecked
        return connector.get(format("%s/typemap", entityUrl), Map.class);
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        connector.post(format("%s/typemap", entityUrl), map, Void.class);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        connector.post(format("%s/holdability", entityUrl), holdability, Void.class);
    }

    @Override
    @JsonIgnore
    public int getHoldability() throws SQLException {
        return connector.get(format("%s/holdability", entityUrl), Integer.class);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return connector.post(format("%s/savepoint", entityUrl), null, TransportableSavepoint.class);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return connector.post(format("%s/savepoint", entityUrl), name, TransportableSavepoint.class);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        connector.post(format("%s/rollback", entityUrl), new TransportableSavepoint(savepoint), Void.class);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        connector.delete(format("%s/savepoint", entityUrl), new TransportableSavepoint(savepoint), Void.class);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connector.post(format("%s/statement?type=%d&concurrency=%d&holdability=%d", entityUrl, resultSetType, resultSetConcurrency, resultSetHoldability), null, StatementProxy.class).withConnection(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connector.post(format("%s/prepared-statement?type=%d&concurrency=%d&holdability=%d", entityUrl, resultSetType, resultSetConcurrency, resultSetHoldability), sql, PreparedStatementProxy.class).withConnection(this);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connector.post(format("%s/callable-statement?type=%d&concurrency=%d&holdability=%d", entityUrl, resultSetType, resultSetConcurrency, resultSetHoldability), sql, CallableStatementProxy.class);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return connector.post(format("%s/prepared-statement?keys=%d", entityUrl, autoGeneratedKeys), sql, PreparedStatementProxy.class).withConnection(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        String indices = Arrays.stream(columnIndexes).mapToObj(i -> "" + i).collect(Collectors.joining(","));
        return connector.post(format("%s/prepared-statement?indexes=%s", entityUrl, indices), sql, PreparedStatementProxy.class).withConnection(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        String names = Arrays.stream(columnNames).map(n -> "" + encode(n)).collect(Collectors.joining(","));
        return connector.post(format("%s/prepared-statement?names=%s", entityUrl, names), sql, PreparedStatementProxy.class).withConnection(this);
    }

    @Override
    public Clob createClob() throws SQLException {
        return connector.post(format("%s/clob", entityUrl), null, ClobProxy.class);
    }

    @Override
    public Blob createBlob() throws SQLException {
        return connector.post(format("%s/blob", entityUrl), null, BlobProxy.class);
    }

    @Override
    public NClob createNClob() throws SQLException {
        return connector.post(format("%s/nclob", entityUrl), null, ClobProxy.class);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return connector.post(format("%s/sqlxml", entityUrl), null, SQLXMLProxy.class);
    }

    @Override
    @JsonIgnore
    public boolean isValid(int timeout) throws SQLException {
        return connector.get(format("%s/valid/%d", entityUrl, timeout), Boolean.class);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        connector.post(format("%s/client/info/%s", entityUrl, encode(name)), encode(value), Void.class);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        connector.post(format("%s/client/info", entityUrl), properties, Void.class);
    }

    @Override
    @JsonIgnore
    public String getClientInfo(String name) throws SQLException {
        return connector.get(format("%s/client/info/%s", entityUrl, encode(name)), String.class);
    }

    @Override
    @JsonIgnore
    public Properties getClientInfo() throws SQLException {
        return connector.get(format("%s/client/info", entityUrl), Properties.class);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return connector.post(format("%s/array/%s", entityUrl, Util.encode(typeName)), elements, ArrayProxy.class);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return connector.post(format("%s/struct/%s", entityUrl, Util.encode(typeName)), attributes, StructProxy.class);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        connector.post(format("%s/schema", entityUrl), schema, Void.class);
    }

    @Override
    @JsonIgnore
    public String getSchema() throws SQLException {
        return connector.get(format("%s/schema", entityUrl), String.class);
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        connector.post(format("%s/abort", entityUrl), System.identityHashCode(executor), Void.class);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        connector.post(format("%s/networktimeout", entityUrl), milliseconds, Void.class);
    }

    @Override
    @JsonIgnore
    public int getNetworkTimeout() throws SQLException {
        return connector.get(format("%s/networktimeout", entityUrl), Integer.class);
    }
}

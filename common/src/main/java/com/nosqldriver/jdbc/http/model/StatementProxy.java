package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.Util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.nosqldriver.jdbc.http.Util.encode;
import static java.lang.String.format;

public class StatementProxy extends WrapperProxy implements Statement {
    private Connection connection;

    @JsonCreator
    public StatementProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return connector.post(format("%s/query", entityUrl), sql, ResultSetProxy.class).withStatement(this);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return connector.post(format("%s/update", entityUrl), sql, Integer.class);
    }

    @Override
    public void close() throws SQLException {
        connector.delete(format("%s", entityUrl), null, Void.class);
    }

    @Override
    @JsonIgnore
    public int getMaxFieldSize() throws SQLException {
        return connector.get(format("%s/maxfieldsize", entityUrl), Integer.class);
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        connector.post(format("%s/maxfieldsize", entityUrl), max, Void.class);
    }

    @Override
    @JsonIgnore
    public int getMaxRows() throws SQLException {
        return connector.get(format("%s/maxrows", entityUrl), Integer.class);
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        connector.post(format("%s/maxrows", entityUrl), max, Void.class);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        connector.post(format("%s/escapeprocessing", entityUrl), enable, Void.class);
    }

    @Override
    @JsonIgnore
    public int getQueryTimeout() throws SQLException {
        return connector.get(format("%s/querytimeout", entityUrl), Integer.class);
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        connector.post(format("%s/querytimeout", entityUrl), seconds, Void.class);
    }

    @Override
    public void cancel() throws SQLException {
        connector.delete(format("%s/cancel", entityUrl), null, Void.class);
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
    public void setCursorName(String name) throws SQLException {
        connector.post(format("%s/cursorname", entityUrl), name, Void.class);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        return connector.post(format("%s/execute", entityUrl), sql, Boolean.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getResultSet() throws SQLException {
        return connector.get(format("%s/resultset", entityUrl), ResultSetProxy.class).withStatement(this);
    }

    @Override
    @JsonIgnore
    public int getUpdateCount() throws SQLException {
        return connector.get(format("%s/updatecount", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public boolean getMoreResults() throws SQLException {
        return connector.get(format("%s/more", entityUrl), Boolean.class);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        connector.post(format("%s/fetch/direction", entityUrl), direction, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchDirection() throws SQLException {
        return connector.get(format("%s/fetch/direction", entityUrl), Integer.class);
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        connector.post(format("%s/fetch/size", entityUrl), rows, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchSize() throws SQLException {
        return connector.get(format("%s/fetch/size", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public int getResultSetConcurrency() throws SQLException {
        return connector.get(format("%s/resultset/concurrency", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public int getResultSetType() throws SQLException {
        return connector.get(format("%s/resultset/type", entityUrl), Integer.class);
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        connector.patch(format("%s/batch", entityUrl), sql, Void.class);
    }

    @Override
    public void clearBatch() throws SQLException {
        connector.delete(format("%s/batch", entityUrl), null, Integer.class);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return connector.post(format("%s/batch", entityUrl), null, int[].class);
    }

    @Override
    @JsonIgnore
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return connector.get(format("%s/more?current=%d", entityUrl, current), Boolean.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getGeneratedKeys() throws SQLException {
        return connector.get(format("%s/generatedkeys", entityUrl), ResultSetProxy.class).withStatement(this);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return connector.post(format("%s/update?keys=%d", entityUrl, autoGeneratedKeys), sql, Integer.class);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        String indices = Arrays.stream(columnIndexes).mapToObj(i -> "" + i).collect(Collectors.joining(","));
        return connector.post(format("%s/update?indexes=%s", entityUrl, indices), sql, Integer.class);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        String columnNamesStr = columnNames == null ? null : Arrays.stream(columnNames).map(Util::encode).collect(Collectors.joining(","));
        return connector.post(format("%s/update?names=%s", entityUrl, columnNamesStr), sql, Integer.class);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return connector.post(format("%s/execute?keys=%d", entityUrl, autoGeneratedKeys), sql, Boolean.class);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        String indices = Arrays.stream(columnIndexes).mapToObj(i -> "" + i).collect(Collectors.joining(","));
        return connector.post(format("%s/execute?indexes=%s", entityUrl, indices), sql, Boolean.class);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        String columnNamesStr = columnNames == null ? null : Arrays.stream(columnNames).map(Util::encode).collect(Collectors.joining(","));
        return connector.post(format("%s/execute?names=%s", entityUrl, columnNamesStr), sql, Boolean.class);
    }

    @Override
    @JsonIgnore
    public int getResultSetHoldability() throws SQLException {
        return connector.get(format("%s/resultset/holdability", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public boolean isClosed() throws SQLException {
        return connector.get(format("%s/closed", entityUrl), Boolean.class);
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        connector.post(format("%s/poolable", entityUrl), poolable, Void.class);
    }

    @Override
    @JsonIgnore
    public boolean isPoolable() throws SQLException {
        return connector.get(format("%s/poolable", entityUrl), Boolean.class);
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        connector.post(format("%s/closeoncompletion", entityUrl), null, Void.class);
    }

    @Override
    @JsonIgnore
    public boolean isCloseOnCompletion() throws SQLException {
        return connector.get(format("%s/closeoncompletion", entityUrl), Boolean.class);
    }

    public StatementProxy withConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    @Override
    @JsonIgnore
    public long getLargeUpdateCount() {
        return connector.get(format("%s/large/updatecount", entityUrl), Long.class);
    }

    @Override
    @JsonIgnore
    public long getLargeMaxRows() throws SQLException {
        return connector.get(format("%s/large/maxrows", entityUrl), Long.class);
    }

    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        connector.post(format("%s/large/maxrows", entityUrl), max, Void.class);
    }

    @Override
    public long[] executeLargeBatch() throws SQLException {
        return connector.post(format("%s/large/batch", entityUrl), null, long[].class);
    }

    @Override
    public long executeLargeUpdate(String sql) throws SQLException {
        return connector.post(format("%s/large/update", entityUrl), sql, Long.class);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return connector.post(format("%s/large/update?keys=%d", entityUrl, autoGeneratedKeys), sql, Long.class);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        String columnIndexesStr = columnIndexes == null ? null : Stream.of(columnIndexes).map(String::valueOf).collect(Collectors.joining(","));
        return connector.post(format("%s/large/update?indexes=%s", entityUrl, columnIndexesStr), sql, Long.class);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        String columnNamesStr = columnNames == null ? null : Arrays.stream(columnNames).map(Util::encode).collect(Collectors.joining(","));
        return connector.post(format("%s/large/update?names=%s", entityUrl, columnNamesStr), sql, Long.class);
    }

    //@Override
    public String enquoteLiteral(String val) throws SQLException {
        return connector.post(format("%s/enquote/literal", entityUrl), val, String.class);
    }

    //@Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) throws SQLException {
        return connector.post(format("%s/enquote/identifier/%s", entityUrl, alwaysQuote), identifier, String.class);
    }

    //@Override
    public boolean isSimpleIdentifier(String identifier) throws SQLException {
        return connector.get(format("%s/simple/identifier/%s", entityUrl, encode(identifier)), Boolean.class);
    }

    //@Override
    public String enquoteNCharLiteral(String val) throws SQLException {
        return connector.post(format("%s/enquote/nchar/literal", entityUrl), val, String.class);
    }
}
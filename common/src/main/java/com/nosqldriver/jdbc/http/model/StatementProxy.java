package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.Util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.nosqldriver.jdbc.http.Util.encode;
import static java.lang.String.format;

public class StatementProxy extends WrapperProxy implements Statement {
    private Connection connection;
    private volatile boolean closed = false;

    @JsonCreator
    public StatementProxy(@JsonProperty("entityUrl") String entityUrl) {
        this(entityUrl, Statement.class);
    }

    protected StatementProxy(String entityUrl, Class<?> clazz) {
        super(entityUrl, clazz);
    }

    @Override
    public ResultSet executeQuery(String sql) {
        return connector.post(format("%s/query", entityUrl), sql, ResultSetProxy.class).withStatement(this);
    }

    @Override
    public int executeUpdate(String sql) {
        return connector.post(format("%s/update", entityUrl), sql, Integer.class);
    }

    @Override
    public void close() {
        connector.delete(format("%s", entityUrl), null, Void.class);
        closed = true;
    }

    @Override
    @JsonIgnore
    public int getMaxFieldSize() {
        return connector.get(format("%s/maxfieldsize", entityUrl), Integer.class);
    }

    @Override
    public void setMaxFieldSize(int max) {
        connector.post(format("%s/maxfieldsize", entityUrl), max, Void.class);
    }

    @Override
    @JsonIgnore
    public int getMaxRows() {
        return connector.get(format("%s/maxrows", entityUrl), Integer.class);
    }

    @Override
    public void setMaxRows(int max) {
        connector.post(format("%s/maxrows", entityUrl), max, Void.class);
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        connector.post(format("%s/escapeprocessing", entityUrl), enable, Void.class);
    }

    @Override
    @JsonIgnore
    public int getQueryTimeout() {
        return connector.get(format("%s/querytimeout", entityUrl), Integer.class);
    }

    @Override
    public void setQueryTimeout(int seconds) {
        connector.post(format("%s/querytimeout", entityUrl), seconds, Void.class);
    }

    @Override
    public void cancel() {
        connector.delete(format("%s/cancel", entityUrl), null, Void.class);
    }

    @Override
    @JsonIgnore
    public SQLWarning getWarnings() {
        return connector.get(format("%s/warnings", entityUrl), TransportableSQLWarning.class);
    }

    @Override
    public void clearWarnings() {
        connector.delete(format("%s/warnings", entityUrl), null, Void.class);
    }

    @Override
    public void setCursorName(String name) {
        connector.post(format("%s/cursorname", entityUrl), name, Void.class);
    }

    @Override
    public boolean execute(String sql) {
        return connector.post(format("%s/execute", entityUrl), sql, Boolean.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getResultSet() {
        return Optional.ofNullable(connector.get(format("%s/resultset", entityUrl), ResultSetProxy.class)).map(rs -> rs.withStatement(this)).orElse(null);
    }

    @Override
    @JsonIgnore
    public int getUpdateCount() {
        return connector.get(format("%s/updatecount", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public boolean getMoreResults() {
        return connector.get(format("%s/more", entityUrl), Boolean.class);
    }

    @Override
    public void setFetchDirection(int direction) {
        connector.post(format("%s/fetch/direction", entityUrl), direction, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchDirection() {
        return connector.get(format("%s/fetch/direction", entityUrl), Integer.class);
    }

    @Override
    public void setFetchSize(int rows) {
        connector.post(format("%s/fetch/size", entityUrl), rows, Void.class);
    }

    @Override
    @JsonIgnore
    public int getFetchSize() {
        return connector.get(format("%s/fetch/size", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public int getResultSetConcurrency() {
        return connector.get(format("%s/resultset/concurrency", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public int getResultSetType() {
        return connector.get(format("%s/resultset/type", entityUrl), Integer.class);
    }

    @Override
    public void addBatch(String sql) {
        connector.put(format("%s/batch", entityUrl), sql, Void.class);
    }

    @Override
    public void clearBatch() {
        connector.delete(format("%s/batch", entityUrl), null, Integer.class);
    }

    @Override
    public int[] executeBatch() {
        return connector.post(format("%s/batch", entityUrl), null, int[].class);
    }

    @Override
    @JsonIgnore
    public Connection getConnection() {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) {
        return connector.get(format("%s/more?current=%d", entityUrl, current), Boolean.class);
    }

    @Override
    @JsonIgnore
    public ResultSet getGeneratedKeys() {
        return Optional.ofNullable(connector.get(format("%s/generatedkeys", entityUrl), ResultSetProxy.class)).map(rs -> rs.withStatement(this)).orElse(null);
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) {
        return connector.post(format("%s/update?keys=%d", entityUrl, autoGeneratedKeys), sql, Integer.class);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) {
        String indices = Arrays.stream(columnIndexes).mapToObj(i -> "" + i).collect(Collectors.joining(","));
        return connector.post(format("%s/update?indexes=%s", entityUrl, indices), sql, Integer.class);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) {
        String columnNamesStr = columnNames == null ? null : Arrays.stream(columnNames).map(Util::encode).collect(Collectors.joining(","));
        return connector.post(format("%s/update?names=%s", entityUrl, columnNamesStr), sql, Integer.class);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) {
        return connector.post(format("%s/execute?keys=%d", entityUrl, autoGeneratedKeys), sql, Boolean.class);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) {
        String indices = Arrays.stream(columnIndexes).mapToObj(i -> "" + i).collect(Collectors.joining(","));
        return connector.post(format("%s/execute?indexes=%s", entityUrl, indices), sql, Boolean.class);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) {
        String columnNamesStr = columnNames == null ? null : Arrays.stream(columnNames).map(Util::encode).collect(Collectors.joining(","));
        return connector.post(format("%s/execute?names=%s", entityUrl, columnNamesStr), sql, Boolean.class);
    }

    @Override
    @JsonIgnore
    public int getResultSetHoldability() {
        return connector.get(format("%s/resultset/holdability", entityUrl), Integer.class);
    }

    @Override
    @JsonIgnore
    public boolean isClosed() {
        return closed || connector.get(format("%s/closed", entityUrl), Boolean.class);
    }

    @Override
    public void setPoolable(boolean poolable) {
        connector.post(format("%s/poolable", entityUrl), poolable, Void.class);
    }

    @Override
    @JsonIgnore
    public boolean isPoolable() {
        return connector.get(format("%s/poolable", entityUrl), Boolean.class);
    }

    @Override
    public void closeOnCompletion() {
        connector.post(format("%s/closeoncompletion", entityUrl), null, Void.class);
    }

    @Override
    @JsonIgnore
    public boolean isCloseOnCompletion() {
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
    public long getLargeMaxRows() {
        return connector.get(format("%s/large/maxrows", entityUrl), Long.class);
    }

    @Override
    public void setLargeMaxRows(long max) {
        connector.post(format("%s/large/maxrows", entityUrl), max, Void.class);
    }

    @Override
    public long[] executeLargeBatch() {
        return connector.post(format("%s/large/batch", entityUrl), null, long[].class);
    }

    @Override
    public long executeLargeUpdate(String sql) {
        return connector.post(format("%s/large/update", entityUrl), sql, Long.class);
    }

    @Override
    public long executeLargeUpdate(String sql, int autoGeneratedKeys) {
        return connector.post(format("%s/large/update?keys=%d", entityUrl, autoGeneratedKeys), sql, Long.class);
    }

    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) {
        String columnIndexesStr = IntStream.of(columnIndexes).mapToObj(i -> ""+i).collect(Collectors.joining(","));
        return connector.post(format("%s/large/update?indexes=%s", entityUrl, columnIndexesStr), sql, Long.class);
    }

    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) {
        String columnNamesStr = columnNames == null ? null : Arrays.stream(columnNames).map(Util::encode).collect(Collectors.joining(","));
        return connector.post(format("%s/large/update?names=%s", entityUrl, columnNamesStr), sql, Long.class);
    }

    @Override
    public String enquoteLiteral(String val) {
        return connector.post(format("%s/enquote/literal", entityUrl), val, String.class);
    }

    @Override
    public String enquoteIdentifier(String identifier, boolean alwaysQuote) {
        return connector.post(format("%s/enquote/identifier/%s", entityUrl, alwaysQuote), identifier, String.class);
    }

    @Override
    public boolean isSimpleIdentifier(String identifier) {
        return connector.get(format("%s/simple/identifier/%s", entityUrl, encode(identifier)), Boolean.class);
    }

    @Override
    public String enquoteNCharLiteral(String val) {
        return connector.post(format("%s/enquote/nchar/literal", entityUrl), val, String.class);
    }
}

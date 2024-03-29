package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;

import static java.lang.String.format;

public class ClobProxy extends EntityProxy implements NClob {
    private final long length;

    @JsonCreator
    public ClobProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl, NClob.class);
        length = -1;
    }

    public ClobProxy(String entityUrl, Clob clob) throws SQLException {
        super(entityUrl, NClob.class);
        length = clob.length();
    }

    @Override
    public long length() {
        return length < 0 ? connector.get(format("%s/length", entityUrl), Long.class) : length;
    }

    @Override
    public String getSubString(long pos, int length) {
        return connector.get(format("%s/substring/%d/%d", entityUrl, pos, length), String.class);
    }

    @Override
    @JsonIgnore
    public Reader getCharacterStream() {
        return connector.get(format("%s/character/stream", entityUrl), ReaderProxy.class);
    }

    @Override
    @JsonIgnore
    public InputStream getAsciiStream() {
        return connector.get(format("%s/ascii/stream", entityUrl), InputStreamProxy.class);
    }

    @Override
    public long position(String searchstr, long start) {
        return positionImpl(searchstr, start);
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        return searchstr instanceof ClobProxy ? positionImpl(searchstr, start) : position(searchstr.getSubString(1, (int)searchstr.length()), start);
    }

    private long positionImpl(Object searchstr, long start) {
        return connector.post(format("%s/position/%d", entityUrl, start), searchstr, Long.class);
    }

    @Override
    public int setString(long pos, String str) {
        return connector.post(format("%s/%d", entityUrl, pos), str, Integer.class);
    }

    @Override
    public int setString(long pos, String str, int offset, int len) {
        return connector.post(format("%s/%d/%d/%d", entityUrl, pos, offset, len), str, Integer.class);
    }

    @Override
    public OutputStream setAsciiStream(long pos) {
        return connector.post(format("%s/ascii/stream/%d", entityUrl, pos), null, OutputStreamProxy.class);
    }

    @Override
    public Writer setCharacterStream(long pos) {
        return connector.post(format("%s/character/stream/%d", entityUrl, pos), null, WriterProxy.class);
    }

    @Override
    public void truncate(long len) {
        connector.delete(entityUrl, len, Void.class);
    }

    @Override
    public void free() {
        connector.delete(entityUrl, null, Void.class);
    }

    @Override
    public Reader getCharacterStream(long pos, long length) {
        return connector.get(format("%s/character/stream/%d/%d", entityUrl, pos, length), ReaderProxy.class);
    }
}

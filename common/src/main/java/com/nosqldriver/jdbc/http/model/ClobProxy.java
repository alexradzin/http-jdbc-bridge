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
    private long length;

    @JsonCreator
    public ClobProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("token") String token) {
        super(entityUrl, token);
    }

    public ClobProxy(String entityUrl, @JsonProperty("token") String token, long length) {
        super(entityUrl, token);
        this.length = length;
    }

    @Override
    public long length() throws SQLException {
        return length;
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        return connector.get(format("%s/substring/%d/%d", entityUrl, pos, length), String.class, token);
    }

    @Override
    @JsonIgnore
    public Reader getCharacterStream() throws SQLException {
        return connector.get(format("%s/character/stream", entityUrl), ReaderProxy.class, token);
    }

    @Override
    @JsonIgnore
    public InputStream getAsciiStream() throws SQLException {
        return connector.get(format("%s/ascii/stream", entityUrl), InputStreamProxy.class, token);
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        return connector.post(format("%s/position/%d", entityUrl, start), searchstr, Long.class, token);
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        return position(searchstr.getSubString(0, (int)searchstr.length()), start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        return connector.put(format("%s/%d", entityUrl, pos), str, Integer.class, token);
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        return connector.put(format("%s/%d/%d/%d", entityUrl, pos, offset, len), str, Integer.class, token);
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        return connector.post(format("%s/ascii/stream/%d", entityUrl, pos), null, OutputStreamProxy.class, token);
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        return connector.post(format("%s/character/stream/%d", entityUrl, pos), null, WriterProxy.class, token);
    }

    @Override
    public void truncate(long len) throws SQLException {
        connector.delete(entityUrl, len, Void.class, token);
    }

    @Override
    public void free() throws SQLException {
        connector.delete(entityUrl, null, Void.class, token);
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return connector.get(format("%s/character/stream/%d/%d", entityUrl, pos, length), ReaderProxy.class, token);
    }
}

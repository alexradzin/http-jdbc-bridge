package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import static java.lang.String.format;

public class BlobProxy extends EntityProxy implements Blob {
    private long length;

    @JsonCreator
    public BlobProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("token") String token) {
        super(entityUrl, token);
    }

    public BlobProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("token") String token, long length) {
        super(entityUrl, token);
        this.length = length;
    }

    @Override
    public long length() throws SQLException {
        return length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        return connector.get(format("%s/bytes/%d/%d", entityUrl, pos, length), byte[].class, token);
    }

    @Override
    @JsonIgnore
    public InputStream getBinaryStream() throws SQLException {
        return connector.get(format("%s/binary/stream", entityUrl), InputStream.class, token);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        return connector.post(format("%s/position/%d", entityUrl, start), pattern, Long.class, token);
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        return position(pattern.getBytes(0, (int)pattern.length()), start);
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        return connector.post(format("%s/bytes/%d", entityUrl, pos), bytes, Integer.class, token);
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        return connector.post(format("%s/bytes/%d/%d/%d", entityUrl, pos, offset, len), bytes, Integer.class, token);
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        return connector.post(format("%s/binary/stream/%d", entityUrl, pos), null, OutputStreamProxy.class, token);
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
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return connector.get(format("%s/binary/stream/%d/%d", entityUrl, pos, length), InputStream.class, token);
    }
}

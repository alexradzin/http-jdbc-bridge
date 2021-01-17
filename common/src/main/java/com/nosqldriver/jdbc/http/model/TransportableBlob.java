package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;

public class TransportableBlob implements Blob {
    private byte[] bytes;

    public TransportableBlob(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public long length() throws SQLException {
        return bytes.length;
    }

    @Override
    public byte[] getBytes(long pos, int length) throws SQLException {
        return Arrays.copyOfRange(bytes, (int)pos - 1, (int)pos - 1 + length);
    }

    @Override
    @JsonIgnore
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(bytes);
    }

    @Override
    public long position(byte[] pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public long position(Blob pattern, long start) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public OutputStream setBinaryStream(long pos) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void truncate(long len) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public InputStream getBinaryStream(long pos, long length) throws SQLException {
        return new ByteArrayInputStream(getBytes(pos, (int)length));
    }
}

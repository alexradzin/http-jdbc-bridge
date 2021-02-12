package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class TransportableClob implements Clob, NClob {
    private String string;
    private final boolean partial;

    public TransportableClob(String string) {
        this(string, false);
    }

    public TransportableClob(String string, boolean partial) {
        this.string = string;
        this.partial = partial;
    }

    @Override
    public long length() throws SQLException {
        throwIfPartial();
        return string.length();
    }

    @Override
    public String getSubString(long pos, int length) throws SQLException {
        throwIfPartial();
        return string.substring((int)pos - 1, length);
    }

    @Override
    @JsonIgnore
    public Reader getCharacterStream() throws SQLException {
        throwIfPartial();
        return new StringReader(string);
    }

    @Override
    @JsonIgnore
    public InputStream getAsciiStream() throws SQLException {
        throwIfPartial();
        return new ByteArrayInputStream(string.getBytes());
    }

    @Override
    public long position(String searchstr, long start) throws SQLException {
        throwIfPartial();
        return string.indexOf(searchstr, (int)start - 1) + 1;
    }

    @Override
    public long position(Clob searchstr, long start) throws SQLException {
        throwIfPartial();
        return position(searchstr.getSubString(1, (int)searchstr.length()), start);
    }

    @Override
    public int setString(long pos, String str) throws SQLException {
        throwIfPartial();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        throwIfPartial();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public OutputStream setAsciiStream(long pos) throws SQLException {
        throwIfPartial();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Writer setCharacterStream(long pos) throws SQLException {
        throwIfPartial();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void truncate(long len) throws SQLException {
        throwIfPartial();
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void free() throws SQLException {
        throwIfPartial();
        string = null;
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        throwIfPartial();
        return new StringReader(getSubString(pos, (int)length));
    }

    private void throwIfPartial() throws SQLException {
        if (partial) {
            throw new SQLException("Cannot retrieve value from blob");
        }
    }
}

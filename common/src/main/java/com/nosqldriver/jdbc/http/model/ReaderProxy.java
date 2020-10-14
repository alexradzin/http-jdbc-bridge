package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

import java.io.IOException;
import java.io.Reader;

import static java.lang.String.format;

public class ReaderProxy extends Reader {
    @JsonProperty
    private final String url;
    @JsonProperty
    protected final String token;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    @JsonCreator
    public ReaderProxy(@JsonProperty("url") String url, @JsonProperty("token") String token) {
        this.url = url;
        this.token = token;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        char[] chars = connector.get(format("%s/%d/%d", url, off, len), char[].class, token);
        System.arraycopy(chars, 0, cbuf, 0, chars.length);
        return chars.length;
    }

    @Override
    public void close() throws IOException {
        connector.delete(url, null, Void.class, token);
    }
}

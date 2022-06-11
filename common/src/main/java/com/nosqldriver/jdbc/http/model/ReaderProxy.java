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
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    @JsonCreator
    public ReaderProxy(@JsonProperty("url") String url) {
        this.url = url;
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        char[] chars = connector.get(format("%s/%d/%d", url, off, len), char[].class);
        if (chars == null) {
            return -1;
        }
        System.arraycopy(chars, 0, cbuf, off, chars.length);
        return chars.length;
    }

    @Override
    public void close() {
        connector.delete(url, null, Void.class);
    }
}

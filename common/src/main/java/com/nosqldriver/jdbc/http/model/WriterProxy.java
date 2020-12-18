package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

import java.io.IOException;
import java.io.Writer;

import static java.lang.String.format;

public class WriterProxy extends Writer {
    @JsonProperty
    private final String url;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    @JsonCreator
    public WriterProxy(@JsonProperty("url") String url) {
        this.url = url;
    }


    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
        connector.put(format("%s/%d/%d", url, off, len), cbuf, Void.class);
    }

    @Override
    public void flush() throws IOException {
        connector.post(url + "/flush", null, Void.class);
    }

    @Override
    public void close() throws IOException {
        connector.delete(url, null, Void.class);
    }
}

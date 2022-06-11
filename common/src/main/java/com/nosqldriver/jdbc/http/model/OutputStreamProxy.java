package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

import java.io.OutputStream;

public class OutputStreamProxy extends OutputStream {
    @JsonProperty
    private final String url;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    @JsonCreator
    public OutputStreamProxy(@JsonProperty("url") String url) {
        this.url = url;
    }

    @Override
    public void write(int b) {
        connector.put(url, b, Void.class);
    }

    @Override
    public void flush() {
        connector.post(url + "/flush", null, Void.class);
    }

    @Override
    public void close() {
        connector.delete(url, null, Void.class);
    }
}

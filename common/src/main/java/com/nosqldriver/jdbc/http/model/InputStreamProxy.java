package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

import java.io.InputStream;

public class InputStreamProxy extends InputStream {
    @JsonProperty
    private final String url;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    @JsonCreator
    public InputStreamProxy(@JsonProperty("url") String url) {
        this.url = url;
    }

    @Override
    public int read() {
        return connector.get(url, int.class);
    }

    @Override
    public void close() {
        connector.delete(url, null, Void.class);
    }
}

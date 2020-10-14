package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamProxy extends OutputStream {
    @JsonProperty
    private final String url;
    @JsonProperty
    protected final String token;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    @JsonCreator
    public OutputStreamProxy(@JsonProperty("url") String url, @JsonProperty("token") String token) {
        this.url = url;
        this.token = token;
    }

    @Override
    public void write(int b) throws IOException {
        connector.put(url, b, Void.class, token);
    }
}

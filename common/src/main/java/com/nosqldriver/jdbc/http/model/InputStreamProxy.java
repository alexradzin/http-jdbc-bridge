package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.nosqldriver.jdbc.http.HttpConnector;

import java.io.IOException;
import java.io.InputStream;

public class InputStreamProxy extends InputStream {
    @JsonProperty
    private final String url;
    @JsonProperty
    protected final String token;
    @JsonIgnore
    protected final HttpConnector connector = new HttpConnector();

    @JsonCreator
    public InputStreamProxy(@JsonProperty("url") String url, @JsonProperty("token") String token) {
        this.url = url;
        this.token = token;
    }

    @Override
    public int read() throws IOException {
        return connector.get(url, int.class, token);
    }
}

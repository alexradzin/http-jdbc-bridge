package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Properties;

public class ConnectionInfo {
    private final String url;
    private final Properties properties;

    @JsonCreator
    public ConnectionInfo(@JsonProperty("url") String url, @JsonProperty("properties") Properties properties) {
        this.url = url;
        this.properties = properties;
    }

    public String getUrl() {
        return url;
    }

    public Properties getProperties() {
        return properties;
    }
}

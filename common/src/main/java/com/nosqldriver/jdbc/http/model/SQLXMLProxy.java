package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.SQLException;
import java.sql.SQLXML;

public class SQLXMLProxy extends EntityProxy implements SQLXML {
    private String string;

    @JsonCreator
    public SQLXMLProxy(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("token") String token) {
        super(entityUrl, token);
    }

    @Override
    public void free() throws SQLException {

    }

    @Override
    @JsonIgnore
    public InputStream getBinaryStream() throws SQLException {
        return null;
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        return null;
    }

    @Override
    @JsonIgnore
    public Reader getCharacterStream() throws SQLException {
        return null;
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        return null;
    }

    @Override
    public String getString() throws SQLException {
        return string;
    }

    @Override
    public void setString(String value) throws SQLException {
        this.string = value;
    }

    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        return null;
    }

    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        return null;
    }
}

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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;

import static java.lang.String.format;

public class SQLXMLProxy extends EntityProxy implements SQLXML {
    @JsonCreator
    public SQLXMLProxy(@JsonProperty("entityUrl") String entityUrl) {
        super(entityUrl);
    }

    @Override
    public void free() throws SQLException {
        connector.delete(entityUrl, null, Void.class);
    }

    @Override
    @JsonIgnore
    public InputStream getBinaryStream() throws SQLException {
        return connector.get(format("%s/binary/stream", entityUrl), InputStreamProxy.class);
    }

    @Override
    public OutputStream setBinaryStream() throws SQLException {
        return connector.post(format("%s/binary/stream", entityUrl), null, OutputStreamProxy.class);
    }

    @Override
    @JsonIgnore
    public Reader getCharacterStream() throws SQLException {
        return connector.get(format("%s/character/stream", entityUrl), ReaderProxy.class);
    }

    @Override
    public Writer setCharacterStream() throws SQLException {
        return connector.post(format("%s/character/stream", entityUrl), null, WriterProxy.class);
    }

    @JsonIgnore
    @Override
    public String getString() throws SQLException {
        return connector.get(format("%s/string", entityUrl), String.class);
    }

    @Override
    public void setString(String value) throws SQLException {
        connector.post(format("%s/string", entityUrl), value, Void.class);
    }

    // Implementation of the next 2 method requires incredible effort relatively to the result because
    // I doubt these methods are very popular. So, this version just throws SQLFeatureNotSupportedException instead.
    @Override
    public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
        throw new SQLFeatureNotSupportedException();
//        return connector.get(format("%s/source/%s", entityUrl, sourceClass), sourceClass);
    }

    @Override
    public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
        throw new SQLFeatureNotSupportedException();
//        return connector.post(format("%s/result", entityUrl), resultClass, resultClass);
    }
}

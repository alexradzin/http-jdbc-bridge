package com.nosqldriver.jdbc.http;

import com.nosqldriver.jdbc.http.model.ConnectionInfo;
import com.nosqldriver.jdbc.http.model.ConnectionProxy;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class HttpDriver implements Driver {
    static {
        try {
            DriverManager.registerDriver(new HttpDriver());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    protected final HttpConnector connector = new HttpConnector();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return acceptsURL(url) ? connector.post(connector.buildUrl(getHttpUrl(url), "connection"), getConnectionInfo(url, info), ConnectionProxy.class) : null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && (url.startsWith("http:") || url.startsWith("https:")) && connector.post(connector.buildUrl(getHttpUrl(url), "acceptsurl"), url, Boolean.class);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String fullUrl, Properties info) throws SQLException {
        Map<String, DriverPropertyInfo> allInfo = new HashMap<>();
        for (String url : fullUrl.split("#")) {
            int questionPos = url.indexOf('?');
            if (questionPos > 0 && questionPos < url.length() - 1) {
                Arrays.stream(url.substring(questionPos + 1).split("&")).forEach(p -> {
                    String[] kv = p.split("=");
                    allInfo.put(kv[0], new DriverPropertyInfo(kv[0], kv.length > 1 ? kv[1] : null));
                });
            }
        }
        allInfo.putAll(info.entrySet().stream().map(e -> new DriverPropertyInfo((String)e.getKey(), (String)e.getValue())).collect(Collectors.toMap(p -> p.name, p -> p)));
        return allInfo.values().toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return Logger.getLogger(getClass().getName());
    }

    private String getHttpUrl(String url) {
        return url.split("#")[0];
    }

    private ConnectionInfo getConnectionInfo(String url, Properties info) {
        String[] parts = url.split("#", 2);
        String jdbcUrl = parts.length > 1 ? parts[1] : null;
        return new ConnectionInfo(jdbcUrl, info);
    }
}

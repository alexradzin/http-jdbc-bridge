package com.nosqldriver.jdbc.mock;

import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class MockDriver implements Driver {
    private static Connection connection;

    private static Connection getConnection() {
        if (connection == null) {
            synchronized (MockDriver.class) {
                if (connection == null) {
                    synchronized (MockDriver.class) {
                        connection = Mockito.mock(Connection.class);
                    }
                }
            }
        }
        return connection;
    }


    static {
        try {
            DriverManager.registerDriver(new MockDriver());
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return acceptsURL(url) ? getConnection() : null;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.startsWith("jdbc:mock");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
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
    public Logger getParentLogger() {
        return Logger.getLogger(getClass().getName());
    }
}

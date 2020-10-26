package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.Test;

import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class HttpDriverTest {
    @Test
    void driverFoundInDriverManager() {
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        HttpDriver httpDriver = null;
        while (drivers.hasMoreElements()) {
            Driver driver = drivers.nextElement();
            if (driver instanceof HttpDriver) {
                httpDriver = (HttpDriver)driver;
            }
        }
        assertNotNull(httpDriver);
    }

    @Test
    void primitiveProperties() throws SQLFeatureNotSupportedException {
        Driver driver = new HttpDriver();
        assertEquals(1, driver.getMajorVersion());
        assertEquals(0, driver.getMinorVersion());
        assertNotNull(driver.getParentLogger());
        assertFalse(driver.jdbcCompliant());
    }

    @Test
    void emptyPropertiesInfo() throws SQLException {
        checkPropertiesInfo("http://localhost#jdbc:h2:mem", new Properties(), Collections.emptyMap());
    }

    @Test
    void somePropertiesInfo() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "myuser");
        props.setProperty("password", "the password");
        Map<String, String> expected = new HashMap<>();
        expected.put("user", "myuser");
        expected.put("password", "the password");
        checkPropertiesInfo("http://localhost#jdbc:h2:mem", props, expected);
    }

    @Test
    void somePropertiesInDbUrlInfo() throws SQLException {
        checkPropertiesInfo("http://localhost#jdbc:h2:mem?fetchSize=1234", new Properties(), Collections.singletonMap("fetchSize", "1234"));
    }

    @Test
    void somePropertiesInHttpUrlInfo() throws SQLException {
        checkPropertiesInfo("http://localhost#jdbc:h2:mem?fetchSize=1234", new Properties(), Collections.singletonMap("fetchSize", "1234"));
    }

    @Test
    void samePropertyInHttpAndDbUrlInfo() throws SQLException {
        checkPropertiesInfo("http://localhost?fetchSize=1234#jdbc:h2:mem?fetchSize=4321", new Properties(), Map.of("fetchSize", "4321"));
    }

    @Test
    void differentPropertiesInHttpAndDbUrlInfo() throws SQLException {
        Map<String, String> expected = new HashMap<>();
        expected.put("fetchSize", "1234");
        expected.put("pageSize", "5678");
        checkPropertiesInfo("http://localhost?fetchSize=1234#jdbc:h2:mem?pageSize=5678", new Properties(), expected);
    }

    @Test
    void differentPropertiesInHttpDbUrlAndProperties() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", "myuser");
        props.setProperty("password", "the password");
        Map<String, String> expected = new HashMap<>();
        expected.put("fetchSize", "1234");
        expected.put("pageSize", "5678");
        expected.put("user", "myuser");
        expected.put("password", "the password");
        checkPropertiesInfo("http://localhost?fetchSize=1234#jdbc:h2:mem?pageSize=5678", props, expected);
    }

    private void checkPropertiesInfo(String url, Properties info, Map<String, String> expected) throws SQLException {
        Driver driver = new HttpDriver();
        Map<String, String> actual = Arrays.stream(driver.getPropertyInfo(url, info)).collect(Collectors.toMap(i -> i.name, i -> i.value));
        assertEquals(expected, actual);
    }
}
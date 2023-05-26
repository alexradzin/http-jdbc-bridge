package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.Configuration;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Stream;

public class JdbcUrlsProvider implements ArgumentsProvider {
    private static final String JDBC_PROP = "jdbc.urls";
    private static final String DEFAULT_JDBC_URLS =
            String.join(",",
                    "jdbc:h2:mem:test",
                    "jdbc:hsqldb:mem",
                    "jdbc:derby:memory:test;create=true",
                    "jdbc:mysql://127.0.0.1:3306/test?user=root",
                    "jdbc:postgresql://localhost:5432/test?user=postgres");
    private static final String JDBC_CONF_PROP = "jdbc.conf";

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
        String jdbcUrls = Configuration.getConfigurationParameter(JDBC_PROP, DEFAULT_JDBC_URLS);
        String[] jdbcUrlsArr = jdbcUrls.split(",");
        Stream<? extends String> urls = Arrays.stream(jdbcUrlsArr);
        String jdbcConf = Configuration.getConfigurationParameter(JDBC_CONF_PROP, null);
        boolean allUrls = "*".equals(jdbcUrls) || "ALL".equals(jdbcUrls);
        if (jdbcConf != null && (allUrls || Arrays.stream(jdbcUrlsArr).anyMatch(s -> !s.startsWith("jdbc:")))) {
            Properties props = new Properties();
            try {
                props.load(new FileInputStream(jdbcConf));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            urls = allUrls ?
                    props.values().stream().map(v -> (String)v) :
                    Arrays.stream(jdbcUrlsArr).map(s -> s.startsWith("jdbc:") ? s : props.getProperty(s));
        }

        return urls.map(Arguments::of);
    }
}

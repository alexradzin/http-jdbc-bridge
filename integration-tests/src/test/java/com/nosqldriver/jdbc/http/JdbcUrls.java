package com.nosqldriver.jdbc.http;

import org.junit.jupiter.params.provider.ValueSource;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@ValueSource(strings = {
        "jdbc:h2:mem:test", "jdbc:hsqldb:mem", "jdbc:derby:memory:test;create=true",
})
public @interface JdbcUrls {
}

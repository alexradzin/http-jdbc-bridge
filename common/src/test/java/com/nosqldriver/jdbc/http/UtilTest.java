package com.nosqldriver.jdbc.http;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UtilTest {
    @Test
    void encodeDecode() {
        String url = "http://host/path?one=first&two=second";
        assertEquals(url, Util.decode(Util.encode(url)));
    }

    @Test
    void nullPathParameter() {
        assertEquals("", Util.pathParameter(null));
    }

    @Test
    void pathParameter() {
        assertEquals("/java.lang.String", Util.pathParameter(String.class));
    }
}

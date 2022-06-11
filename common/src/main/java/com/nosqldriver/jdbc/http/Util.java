package com.nosqldriver.jdbc.http;

import com.nosqldriver.util.function.ThrowingSupplier;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Util {
    public static String encode(String s) {
        return code(() -> URLEncoder.encode(s, UTF_8.name()));
    }

    public static String decode(String s) {
        return code(() -> URLDecoder.decode(s, UTF_8.name()));
    }

    private static String code(ThrowingSupplier<String, UnsupportedEncodingException> coder) {
        try {
            return coder.get();
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String pathParameter(Class<?> clazz) {
        return clazz == null ? "" : "/" + encode(clazz.getName());
    }

    public static byte[] toByteArray(InputStream is) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16 * 1024];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        return buffer.toByteArray();
    }
}

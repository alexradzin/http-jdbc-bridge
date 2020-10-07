package com.nosqldriver.jdbc.http;

import spark.Request;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;

public class Util {
    public static String encode(String s) {
        try {
            return URLEncoder.encode(s, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static String pathParameter(Class<?> clazz) {
        return clazz == null ? "" : "/" + encode(clazz.getName());
    }

    public static Integer toInt(String str) {
        return str == null ? null : Integer.parseInt(str);
    }


    public static int[] toIntArray(String str) {
        return str == null ? null : Arrays.stream(str.split(",")).mapToInt(Integer::parseInt).toArray();
    }

    public static String[] toStringArray(String str) {
        return str == null ? null : str.split(",");
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

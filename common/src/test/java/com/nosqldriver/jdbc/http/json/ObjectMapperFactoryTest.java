package com.nosqldriver.jdbc.http.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.RowData;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ObjectMapperFactoryTest {
    @Test
    void noData() throws JsonProcessingException {
        test(new RowData(true, false, false, new Object[0]));
    }

    @Test
    void string() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {"hello"}));
    }

    @Test
    void integer() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {123}));
    }

    @Test
    void floatingPointNumber() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {3.14}));
    }

    @Test
    void booleanValue() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {true}));
    }

    @Test
    void time() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {Time.valueOf("22:33:44")}));
    }

    @Test
    void date() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {new Date(System.currentTimeMillis())}));
    }

    @Test
    void timestamp() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {new Timestamp(System.currentTimeMillis())}));
    }

    @Test
    void several() throws JsonProcessingException {
        test(new RowData(true, true, true, new Object[] {
                "hello", 345, 2.7, 3.1415926,
                Time.valueOf("22:33:44"),
                new Date(System.currentTimeMillis()),
                new Timestamp(System.currentTimeMillis())
        }));
    }

    void test(RowData in) throws JsonProcessingException {
        ObjectMapper objectMapper = ObjectMapperFactory.createObjectMapper();
        RowData out = objectMapper.readValue(objectMapper.writeValueAsString(in), RowData.class);
        assertEquals(in, out);
    }

}
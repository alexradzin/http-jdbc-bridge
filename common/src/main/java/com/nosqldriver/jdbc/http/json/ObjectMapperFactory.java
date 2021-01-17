package com.nosqldriver.jdbc.http.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Collections;

public class ObjectMapperFactory {
    public static ObjectMapper createObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        SimpleModule module = new SimpleModule();
        module.addDeserializer(Object.class, new JdbcDeserializer(Object.class));

        module.addSerializer(Time.class, new JdbcSerializer<>(Time.class, time -> Collections.singletonMap("time", time.toString())));
        module.addSerializer(Timestamp.class, new JdbcSerializer<>(Timestamp.class, time -> Collections.singletonMap("epoch", time.getTime())));
        module.addSerializer(Date.class, new JdbcSerializer<>(Date.class, time -> Collections.singletonMap("epoch", time.getTime())));
        objectMapper.registerModule(module);

        return objectMapper;
    }
}

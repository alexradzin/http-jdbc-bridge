package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.SQLWarning;

public class TransportableSQLWarning extends SQLWarning {
    @JsonCreator
    public TransportableSQLWarning(
            @JsonProperty("message") String message,
            @JsonProperty("cause") Throwable cause,
            @JsonProperty("stackTrace") StackTraceElement[] stackTrace,
            @JsonProperty("next") SQLWarning next,
            @JsonProperty("SQLState") String sqlState,
            @JsonProperty("errorCode") int errorCode) {
        super(message, cause == null ? null : new TransportableSQLWarning(cause.getMessage(), cause.getCause(), cause.getStackTrace(), next, sqlState, errorCode));
        setStackTrace(stackTrace);
        setNextWarning(next);
        super.getErrorCode();
    }

    public TransportableSQLWarning(SQLWarning w) {
        this(w.getMessage(), w.getCause(), w.getStackTrace(), w.getNextWarning(), w.getSQLState(), w.getErrorCode());
    }

    @Override
    @JsonProperty("SQLState")
    public String getSQLState() {
        return super.getSQLState();
    }

    public static void main(String[] args) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        Throwable t1 = new TransportableSQLWarning("mmm", null, new Throwable().getStackTrace(), null, "state", 123);
        String json = mapper.writeValueAsString(t1);
        System.out.println(json);
        Throwable t2 = mapper.readValue(json, TransportableSQLWarning.class);
        String json2 = mapper.writeValueAsString(t2);
        System.out.println(json2);
    }
}

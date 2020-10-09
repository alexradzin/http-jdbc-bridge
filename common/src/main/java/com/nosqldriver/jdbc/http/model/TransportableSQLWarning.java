package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

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

}

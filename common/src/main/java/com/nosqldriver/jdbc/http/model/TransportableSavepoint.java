package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.SQLException;
import java.sql.Savepoint;

public class TransportableSavepoint implements Savepoint {
    private final int savePointId;
    private final String savePointName;

    public TransportableSavepoint(Savepoint savepoint) throws SQLException {
        this(savepoint.getSavepointId(), savepoint.getSavepointName());
    }

    @JsonCreator
    public TransportableSavepoint(@JsonProperty("savePointId") int savePointId, @JsonProperty("savePointName") String savePointName) {
        this.savePointId = savePointId;
        this.savePointName = savePointName;
    }


    @Override
    public int getSavepointId() throws SQLException {
        return savePointId;
    }

    @Override
    public String getSavepointName() throws SQLException {
        return savePointName;
    }
}

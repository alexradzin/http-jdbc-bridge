package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.sql.SQLException;
import java.sql.Savepoint;

public class TransportableSavepoint extends EntityProxy implements Savepoint {
    @JsonProperty private final int savepointId;
    @JsonProperty private final String savepointName;

    public TransportableSavepoint(String entityUrl, Savepoint savepoint) {
        this(entityUrl, retrieveSavepointId(savepoint), retrieveSavepointName(savepoint));
    }

    /**
     * Some implementations throw exceptions when {@link Savepoint#getSavepointName()} is called on unnamed savepoint.
     * @param savepoint original savepoint
     * @return name or null
     */
    private static String retrieveSavepointName(Savepoint savepoint) {
        try {
            return savepoint.getSavepointName();
        } catch (SQLException e) {
            return null;
        }
    }

    /**
     * Some implementations throw exceptions when {@link Savepoint#getSavepointId()} is called on named savepoint.
     * @param savepoint original savepoint
     * @return name or 0
     */
    private static int retrieveSavepointId(Savepoint savepoint) {
        try {
            return savepoint.getSavepointId();
        } catch (SQLException e) {
            return 0;
        }
    }

    @JsonCreator
    public TransportableSavepoint(@JsonProperty("entityUrl") String entityUrl, @JsonProperty("savepointId") int savepointId, @JsonProperty("savepointName") String savepointName) {
        super(entityUrl, Savepoint.class);
        this.savepointId = savepointId;
        this.savepointName = savepointName;
    }


    @Override
    public int getSavepointId() {
        return savepointId;
    }

    @Override
    public String getSavepointName() {
        return savepointName;
    }
}

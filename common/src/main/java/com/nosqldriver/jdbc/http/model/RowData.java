package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RowData {
    private final boolean moved;
    private final Object[] row;

    @JsonCreator
    public RowData(@JsonProperty("moved") boolean moved, @JsonProperty("row") Object[] row) {
        this.moved = moved;
        this.row = row;
    }

    public boolean isMoved() {
        return moved;
    }

    public Object[] getRow() {
        return row;
    }
}

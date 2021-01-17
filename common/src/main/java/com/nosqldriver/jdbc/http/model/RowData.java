package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public class RowData {
    @JsonProperty private final boolean moved;
    @JsonProperty private final Object[] row;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowData rowData = (RowData) o;
        return moved == rowData.moved && Arrays.equals(row, rowData.row);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(moved);
        result = 31 * result + Arrays.hashCode(row);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RowData.class.getSimpleName() + "[", "]")
                .add("moved=" + moved)
                .add("row=" + Arrays.toString(row))
                .toString();
    }
}

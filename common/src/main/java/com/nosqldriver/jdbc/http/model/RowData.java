package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;

public class RowData {
    @JsonProperty private final boolean moved;
    @JsonProperty private final Boolean first;
    @JsonProperty private final Boolean last;
    @JsonProperty private final Object[] row;

    @JsonCreator
    public RowData(@JsonProperty("moved") boolean moved,
                   @JsonProperty("first") Boolean first,
                   @JsonProperty("last") Boolean last,
                   @JsonProperty("row") Object[] row) {
        this.moved = moved;
        this.first = first;
        this.last = last;
        this.row = row;
    }

    public boolean isMoved() {
        return moved;
    }

    public Boolean getFirst() {
        return first;
    }

    public Boolean getLast() {
        return last;
    }

    public Object[] getRow() {
        return row;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowData rowData = (RowData) o;
        return moved == rowData.moved &&
               first == rowData.first &&
               last == rowData.last &&
               Arrays.equals(row, rowData.row);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(moved, first, last);
        result = 31 * result + Arrays.hashCode(row);
        return result;
    }

    @Override
    public String toString() {
        return new StringJoiner(", ", RowData.class.getSimpleName() + "[", "]")
                .add("moved=" + moved)
                .add("first=" + first)
                .add("last=" + last)
                .add("row=" + Arrays.toString(row))
                .toString();
    }
}

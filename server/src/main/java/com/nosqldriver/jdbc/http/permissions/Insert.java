package com.nosqldriver.jdbc.http.permissions;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

public class Insert {
    private String table;
    private List<String> fields;
    private Optional<Long> limit = Optional.empty();

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public Optional<Long> getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = Optional.of(limit);
    }
}

package com.nosqldriver.jdbc.http.permissions;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

public class Delete {
    private String table;
    private List<Entry<String, ComparisonOperation>> where;

    public List<Entry<String, ComparisonOperation>> getWhere() {
        return where;
    }

    public void setWhere(List<Entry<String, ComparisonOperation>> where) {
        this.where = where;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }
}

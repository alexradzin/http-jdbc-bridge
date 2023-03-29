package com.nosqldriver.jdbc.http.permissions;

import java.util.List;
import java.util.Map.Entry;

public class Update {
    private String table;
    private List<String> fields;
    private List<Entry<String, ComparisonOperation>> where;

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

    public List<Entry<String, ComparisonOperation>> getWhere() {
        return where;
    }

    public void setWhere(List<Entry<String, ComparisonOperation>> where) {
        this.where = where;
    }
}

package com.nosqldriver.jdbc.http.permissions;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;

public class Select {
    private String table;
    private List<String> fields;
    private List<Entry<String, ComparisonOperation>> where;
    private List<String> groupBy = List.of();
    private List<String> orderBy = List.of();
    private Optional<Long> limit = Optional.empty();
    private List<String> innerJoins = List.of();
    private List<String> outerJoins = List.of();


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

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public List<String> getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(List<String> orderBy) {
        this.orderBy = orderBy;
    }

    public Optional<Long> getLimit() {
        return limit;
    }

    public void setLimit(long limit) {
        this.limit = Optional.of(limit);
    }

    public List<String> getInnerJoins() {
        return innerJoins;
    }

    public void setInnerJoins(List<String> innerJoins) {
        this.innerJoins = innerJoins;
    }

    public List<String> getOuterJoins() {
        return outerJoins;
    }

    public void setOuterJoins(List<String> outerJoins) {
        this.outerJoins = outerJoins;
    }
}

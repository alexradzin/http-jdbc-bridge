package com.nosqldriver.jdbc.http.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ColumnMetaData {
    private final String label;
    private final String name;
    private final String catalog;
    private final String schema;
    private final String table;

    private final int type;
    private final String typeName;
    private final String className;

    private final boolean autoIncrement;
    private final boolean caseSensitive;
    private final boolean searchable;
    private final boolean currency;
    private final int nullable;
    private final boolean signed;
    private final int displaySize;

    private final int precision;
    private final int scale;
    private final boolean readOnly;
    private final boolean writable;
    private final boolean definitelyWritable;

    @JsonCreator
    public ColumnMetaData(@JsonProperty("label") String label,
                          @JsonProperty("name") String name,
                          @JsonProperty("catalog") String catalog,
                          @JsonProperty("schema") String schema,
                          @JsonProperty("table") String table,
                          @JsonProperty("type") int type,
                          @JsonProperty("typeName") String typeName,
                          @JsonProperty("className") String className,
                          @JsonProperty("autoIncrement") boolean autoIncrement,
                          @JsonProperty("caseSensitive") boolean caseSensitive,
                          @JsonProperty("searchable") boolean searchable,
                          @JsonProperty("currency") boolean currency,
                          @JsonProperty("nullable") int nullable,
                          @JsonProperty("signed") boolean signed,
                          @JsonProperty("displaySize") int displaySize,
                          @JsonProperty("precision") int precision,
                          @JsonProperty("scale") int scale,
                          @JsonProperty("readOnly") boolean readOnly,
                          @JsonProperty("writable") boolean writable,
                          @JsonProperty("definitelyWritable") boolean definitelyWritable) {
        this.label = label;
        this.name = name;
        this.catalog = catalog;
        this.schema = schema;
        this.table = table;
        this.type = type;
        this.typeName = typeName;
        this.className = className;
        this.autoIncrement = autoIncrement;
        this.caseSensitive = caseSensitive;
        this.searchable = searchable;
        this.currency = currency;
        this.nullable = nullable;
        this.signed = signed;
        this.displaySize = displaySize;
        this.precision = precision;
        this.scale = scale;
        this.readOnly = readOnly;
        this.writable = writable;
        this.definitelyWritable = definitelyWritable;
    }

    public String getLabel() {
        return label;
    }

    public String getName() {
        return name;
    }

    public String getCatalog() {
        return catalog;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public int getType() {
        return type;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getClassName() {
        return className;
    }

    public boolean isAutoIncrement() {
        return autoIncrement;
    }

    public boolean isCaseSensitive() {
        return caseSensitive;
    }

    public boolean isSearchable() {
        return searchable;
    }

    public boolean isCurrency() {
        return currency;
    }

    public int isNullable() {
        return nullable;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getDisplaySize() {
        return displaySize;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public boolean isWritable() {
        return writable;
    }

    public boolean isDefinitelyWritable() {
        return definitelyWritable;
    }
}

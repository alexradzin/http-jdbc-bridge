package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.util.function.ThrowingBiFunction;
import spark.Request;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.lang.String.format;
import static spark.Spark.get;

public class ResultSetMetaDataController extends BaseController {
    protected ResultSetMetaDataController(Map<String, Object> attributes, ObjectMapper objectMapper, String parentUrl) {
        super(attributes, objectMapper);
        String baseUrl = parentUrl + "/metadata/:metadata";
        get(format("%s/column/count", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), ResultSetMetaData::getColumnCount));
        get(format("%s/:field/:index", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), rs -> {
            return getOrThrow(getterByIndex, req.params(":field"), k -> new IllegalArgumentException(format("Unsupported field '%s'",k))).apply(rs, intParam(req, ":index"));
        }));
        get(format("%s/:category/:field/:index", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), rs -> {
            String id = format("%s/%s", req.params(":category"), req.params(":field"));
            return getOrThrow(getterByIndex, id, k -> new IllegalArgumentException(format("Unsupported field '%s'", k))).apply(rs, intParam(req, ":index"));
        }));

    }

    private ResultSetMetaData getMetadata(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "metadata", ":metadata");
    }

    private static final Map<String, ThrowingBiFunction<ResultSetMetaData, Integer, ?, SQLException>> getterByIndex = new HashMap<>();
    static {
        getterByIndex.put("autoincrement", ResultSetMetaData::isAutoIncrement);
        getterByIndex.put("casesensitive", ResultSetMetaData::isCaseSensitive);
        getterByIndex.put("searchable", ResultSetMetaData::isSearchable);
        getterByIndex.put("currency", ResultSetMetaData::isCurrency);
        getterByIndex.put("nullable", ResultSetMetaData::isNullable);
        getterByIndex.put("signed", ResultSetMetaData::isSigned);
        getterByIndex.put("precision", ResultSetMetaData::getPrecision);
        getterByIndex.put("scale", ResultSetMetaData::getScale);
        getterByIndex.put("readonly", ResultSetMetaData::isReadOnly);
        getterByIndex.put("writable", ResultSetMetaData::isWritable);
        getterByIndex.put("definitelywritable", ResultSetMetaData::isDefinitelyWritable);

        getterByIndex.put("catalog/name", ResultSetMetaData::getCatalogName);
        getterByIndex.put("schema/name", ResultSetMetaData::getSchemaName);
        getterByIndex.put("table/name", ResultSetMetaData::getTableName);

        getterByIndex.put("column/type", ResultSetMetaData::getColumnType);
        getterByIndex.put("column/typename", ResultSetMetaData::getColumnTypeName);
        getterByIndex.put("column/classname", ResultSetMetaData::getColumnClassName);
        getterByIndex.put("column/name", ResultSetMetaData::getColumnName);
        getterByIndex.put("column/label", ResultSetMetaData::getColumnLabel);
        getterByIndex.put("column/displaysize", ResultSetMetaData::getColumnDisplaySize);
    }

    private <K, V, E extends RuntimeException> V getOrThrow(Map<K, V> map, K key, Function<K, E> exceptionFactory) {
        return Optional.ofNullable(map.get(key)).orElseThrow(() -> exceptionFactory.apply(key));
    }

}

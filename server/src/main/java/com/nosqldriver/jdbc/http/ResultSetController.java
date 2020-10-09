package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ParameterValue;
import com.nosqldriver.jdbc.http.model.ResultSetMetaDataProxy;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingTriConsumer;
import spark.Request;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import static java.lang.String.format;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.patch;
import static spark.Spark.post;
import static spark.Spark.put;

public class ResultSetController extends BaseController {
    private final String prefix;
    private final String id;

    protected ResultSetController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);

        String[] urlParts =  baseUrl.split("/");
        prefix = urlParts[urlParts.length - 2];
        id = urlParts[urlParts.length - 1];

        delete(format("%s", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::close));

        get(format("%s/metadata", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::getMetaData, ResultSetMetaDataProxy::new, "metadata", req.url()));

        get(format("%s/next", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::next));
        get(format("%s/previous", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::previous));
        get(format("%s/absolute/:row", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> rs.absolute(intParam(req, "row"))));
        get(format("%s/relative/:row", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> rs.relative(intParam(req, "row"))));

        post(format("%s/move", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), rs -> {
            switch (req.body()) {
                case "current": rs.moveToCurrentRow(); break;
                case "insert": rs.moveToInsertRow(); break;
                default: throw new IllegalArgumentException("Move parameter '%s' is wrong. Supported values are 'current' and 'insert'");
            }
        }));

        post(format("%s/before/first", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::beforeFirst));
        post(format("%s/first", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::first));
        post(format("%s/after/last", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::afterLast));
        post(format("%s/last", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::last));
        get(format("%s/before/first", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isBeforeFirst));
        get(format("%s/first", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isFirst));
        get(format("%s/after/last", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isAfterLast));
        get(format("%s/last", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isLast));

        get(format("%s/fetch/size", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::getFetchSize));
        post(format("%s/fetch/size", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), rs -> rs.setFetchSize(Integer.parseInt(req.body()))));

        get(format("%s/fetch/direction", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::getFetchDirection));
        post(format("%s/fetch/direction", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), rs -> rs.setFetchDirection(Integer.parseInt(req.body()))));

        get(format("%s/type", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::getType));
        get(format("%s/concurrency", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::getConcurrency));

        get(format("%s/row/updated", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::rowUpdated));
        get(format("%s/row/inserted", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::rowInserted));
        get(format("%s/row/deleted", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::rowDeleted));

        get(format("%s/row", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::refreshRow));
        post(format("%s/row", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::insertRow));
        put(format("%s/row", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), rs -> {if("cancel".equals(req.body())) rs.cancelRowUpdates(); else rs.updateRow();}));
        delete(format("%s/row", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::deleteRow));

        get(format("%s/wasnull", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::wasNull));

        get(format("%s/:type/index/:index", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> {
            return getOrThrow(getterByIndex, req.params(":type"), k -> new IllegalArgumentException(format("Unsupported column type '%s'",k))).apply(rs, intParam(req, ":index"));
        }));

        get(format("%s/:type/label/:label", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> {
            return getOrThrow(getterByLabel, req.params(":type"), k -> new IllegalArgumentException(format("Unsupported column type '%s'",k))).apply(rs, req.params(":label"));
        }));

        patch(baseUrl, JSON, (req, res) -> accept(() -> getResultSet(attributes, req), rs -> {
            ParameterValue parameterValue = objectMapper.readValue(req.body(), ParameterValue.class);
            String label = parameterValue.getName();
            String typeName = parameterValue.getTypeName();
            if (label != null) {
                updateByLabel.get(typeName).accept(rs, label, parameterValue.getValue());
            } else {
                int index = parameterValue.getIndex();
                updateByIndex.get(typeName).accept(rs, index, parameterValue.getValue());
            }
        }));

        new ResultSetMetaDataController(attributes, objectMapper, baseUrl);
    }

    private static final Map<String, ThrowingBiFunction<ResultSet, Integer, ?, SQLException>> getterByIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        getterByIndex.put("String", ResultSet::getString);
        getterByIndex.put("NString", ResultSet::getNString);
        getterByIndex.put("byte", ResultSet::getByte);
        getterByIndex.put("short", ResultSet::getShort);
        getterByIndex.put("int", ResultSet::getInt);
        getterByIndex.put("long", ResultSet::getLong);
        getterByIndex.put("boolean", ResultSet::getBoolean);
        getterByIndex.put("float", ResultSet::getFloat);
        getterByIndex.put("double", ResultSet::getDouble);
        getterByIndex.put("bigdecimal", ResultSet::getBigDecimal);
        getterByIndex.put("date", ResultSet::getDate);
        getterByIndex.put("time", ResultSet::getTime);
        getterByIndex.put("timestamp", ResultSet::getTimestamp);
        getterByIndex.put("url", ResultSet::getURL);

        getterByIndex.put("array", ResultSet::getArray);
        getterByIndex.put("blob", ResultSet::getBlob);
        getterByIndex.put("clob", ResultSet::getClob);
        getterByIndex.put("nclob", ResultSet::getNClob);
        getterByIndex.put("bytes", ResultSet::getBytes);
        getterByIndex.put("ref", ResultSet::getRef);

        getterByIndex.put("AsciiStream", ResultSet::getAsciiStream);
        getterByIndex.put("BinaryStream", ResultSet::getBinaryStream);
        getterByIndex.put("CharacterStream", ResultSet::getCharacterStream);
        getterByIndex.put("NCharacterStream", ResultSet::getNCharacterStream);
        getterByIndex.put("UnicodeStream", ResultSet::getUnicodeStream);

        getterByIndex.put("Object", ResultSet::getObject);
    }

    private static final Map<String, ThrowingBiFunction<ResultSet, String, ?, SQLException>> getterByLabel = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        getterByLabel.put("String", ResultSet::getString);
        getterByLabel.put("NString", ResultSet::getNString);
        getterByLabel.put("byte", ResultSet::getByte);
        getterByLabel.put("short", ResultSet::getShort);
        getterByLabel.put("int", ResultSet::getInt);
        getterByLabel.put("long", ResultSet::getLong);
        getterByLabel.put("boolean", ResultSet::getBoolean);
        getterByLabel.put("float", ResultSet::getFloat);
        getterByLabel.put("double", ResultSet::getDouble);
        getterByLabel.put("date", ResultSet::getDate);
        getterByLabel.put("time", ResultSet::getTime);
        getterByLabel.put("timestamp", ResultSet::getTimestamp);
        getterByLabel.put("url", ResultSet::getURL);

        getterByLabel.put("array", ResultSet::getArray);
        getterByLabel.put("blob", ResultSet::getBlob);
        getterByLabel.put("clob", ResultSet::getClob);
        getterByLabel.put("nclob", ResultSet::getNClob);
        getterByLabel.put("bytes", ResultSet::getBytes);
        getterByLabel.put("ref", ResultSet::getRef);

        getterByLabel.put("AsciiStream", ResultSet::getAsciiStream);
        getterByLabel.put("BinaryStream", ResultSet::getBinaryStream);
        getterByLabel.put("CharacterStream", ResultSet::getCharacterStream);
        getterByLabel.put("NCharacterStream", ResultSet::getNCharacterStream);
        getterByLabel.put("UnicodeStream", ResultSet::getUnicodeStream);

        getterByLabel.put("Object", ResultSet::getObject);
    }

    private static final Map<String, ThrowingTriConsumer<ResultSet, Integer, Object, SQLException>> updateByIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        updateByIndex.put("String", (rs, i, v) -> rs.updateString(i, (String)v));
        updateByIndex.put("byte", (rs, i, v) -> rs.updateByte(i, (byte)v));
        updateByIndex.put("short", (rs, i, v) -> rs.updateShort(i, (short)v));
        updateByIndex.put("int", (rs, i, v) -> rs.updateInt(i, (int)v));
        updateByIndex.put("long", (rs, i, v) -> rs.updateLong(i, (long)v));
        updateByIndex.put("float", (rs, i, v) -> rs.updateFloat(i, (float)v));
        updateByIndex.put("double", (rs, i, v) -> rs.updateDouble(i, (double)v));
        updateByIndex.put("boolean", (rs, i, v) -> rs.updateBoolean(i, (boolean)v));
        updateByIndex.put(Date.class.getSimpleName(), (rs, i, v) -> rs.updateDate(i, (Date)v));
        updateByIndex.put(Time.class.getSimpleName(), (rs, i, v) -> rs.updateTime(i, (Time)v));
        updateByIndex.put(Timestamp.class.getSimpleName(), (rs, i, v) -> rs.updateTimestamp(i, (Timestamp)v));
        // TODO: add clob, blob etc
    }

    private static final Map<String, ThrowingTriConsumer<ResultSet, String, Object, SQLException>> updateByLabel = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        updateByLabel.put("String", (rs, i, v) -> rs.updateString(i, (String)v));
        updateByLabel.put("byte", (rs, i, v) -> rs.updateByte(i, (byte)v));
        updateByLabel.put("short", (rs, i, v) -> rs.updateShort(i, (short)v));
        updateByLabel.put("int", (rs, i, v) -> rs.updateInt(i, (int)v));
        updateByLabel.put("long", (rs, i, v) -> rs.updateLong(i, (long)v));
        updateByLabel.put("float", (rs, i, v) -> rs.updateFloat(i, (float)v));
        updateByLabel.put("double", (rs, i, v) -> rs.updateDouble(i, (double)v));
        updateByLabel.put("boolean", (rs, i, v) -> rs.updateBoolean(i, (boolean)v));
        updateByLabel.put(Date.class.getSimpleName(), (rs, i, v) -> rs.updateDate(i, (Date)v));
        updateByLabel.put(Time.class.getSimpleName(), (rs, i, v) -> rs.updateTime(i, (Time)v));
        updateByLabel.put(Timestamp.class.getSimpleName(), (rs, i, v) -> rs.updateTimestamp(i, (Timestamp)v));
        // TODO: add clob, blob etc
    }


    private ResultSet getResultSet(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, prefix, id);
    }

    private <K, V, E extends RuntimeException> V getOrThrow(Map<K, V> map, K key, Function<K, E> exceptionFactory) {
        return Optional.ofNullable(map.get(key)).orElseThrow(() -> exceptionFactory.apply(key));
    }

}

package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ArrayProxy;
import com.nosqldriver.jdbc.http.model.BlobProxy;
import com.nosqldriver.jdbc.http.model.ClobProxy;
import com.nosqldriver.jdbc.http.model.ConnectionProxy;
import com.nosqldriver.jdbc.http.model.ParameterValue;
import com.nosqldriver.jdbc.http.model.RowData;
import com.nosqldriver.jdbc.http.model.TransportableResultSetMetaData;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingFunction;
import com.nosqldriver.util.function.ThrowingTriConsumer;
import spark.Request;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import static java.lang.String.format;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

public class ResultSetController extends BaseController {
    private final String prefix;
    private final String id;

    protected ResultSetController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl, boolean withComplexTypes) {
        super(attributes, objectMapper);

        String[] urlParts =  baseUrl.split("/");
        prefix = urlParts[urlParts.length - 2];
        id = urlParts[urlParts.length - 1];

        delete(format("%s", baseUrl), JSON, (req, res) -> accept(() -> getResultSet(attributes, req), ResultSet::close));

        get(format("%s/metadata", baseUrl), JSON, (req, res) -> retrieve2(() -> getResultSet(attributes, req), ResultSet::getMetaData, TransportableResultSetMetaData::new, "metadata", req.url()));

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
        get(format("%s/is/before/first", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isBeforeFirst));
        get(format("%s/is/first", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isFirst));
        get(format("%s/is/after/last", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isAfterLast));
        get(format("%s/is/last", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), ResultSet::isLast));

        ///////// navigation with cache
        get(format("%s/nextrow", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> move(rs, ResultSet::next, req.url())));
        get(format("%s/previousrow", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> move(rs, ResultSet::previous, req.url())));
        get(format("%s/firstrow", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> move(rs, ResultSet::first, req.url())));
        get(format("%s/lastrow", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> move(rs, ResultSet::last, req.url())));
        get(format("%s/absoluterow/:row", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> move(rs, r -> r.absolute(intParam(req, "row")), req.url())));
        get(format("%s/relativerow/:row", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> move(rs, r -> r.relative(intParam(req, "row")), req.url())));


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
        get(format("%s/column/label/:label", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), rs -> rs.findColumn(req.params(":label"))));

        mapGetters(attributes, baseUrl, "index", getterByIndex, req -> intParam(req, ":index"));
        mapGetters(attributes, baseUrl, "label", getterByLabel, req -> req.params(":label"));

        put(baseUrl, JSON, (req, res) -> accept(() -> getResultSet(attributes, req), rs -> {
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

        get(format("%s/wrapper/:class", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), w -> w.isWrapperFor(Class.forName(req.params(":class")))));
        get(format("%s/unwrap/:class", baseUrl), JSON, (req, res) -> retrieve(() -> getResultSet(attributes, req), w -> w.unwrap(Class.forName(req.params(":class"))), ConnectionProxy::new, "resultset", parentUrl(req.url())));

        if (withComplexTypes) {
            new ArrayController(attributes, objectMapper, format("%s/array/:array", baseUrl));
            new BlobController(attributes, objectMapper, format("%s/blob/:blob", baseUrl));
            new ClobController(attributes, objectMapper, format("%s/clob/:clob", baseUrl));
            new ClobController(attributes, objectMapper, format("%s/nclob/:nclob", baseUrl));
        }
    }

    private <T> void mapGetters(Map<String, Object> attributes, String baseUrl, String markerName, Map<String, ThrowingBiFunction<ResultSet, T, ?, SQLException>> getterByMarker, Function<Request, T> markerValueGetter) {
        get(format("%s/:type/%s/:%s", baseUrl, markerName, markerName), JSON, (req, resp) -> {
            String type = req.params(":type");
            ThrowingBiFunction<ResultSet, T, Object, SQLException> getter = (ThrowingBiFunction<ResultSet, T, Object, SQLException>)getOrThrow(getterByMarker, type, k -> new IllegalArgumentException(format("Unsupported column type '%s'",k)));
            ThrowingBiFunction<String, Object, Object, Exception> transformer = transformers.get(type);

            T labelValue = markerValueGetter.apply(req);
            if(transformer == null) {
                return retrieve(() -> getResultSet(attributes, req), rs -> getter.apply(rs, labelValue));
            } else {
                return retrieve2(() -> getResultSet(attributes, req), rs -> getter.apply(rs, labelValue), transformer, type, req.url());
            }
        });
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

    private static final Map<String, ThrowingBiFunction<String, Object, Object, Exception>> transformers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        transformers.put("array", (url, a) -> new ArrayProxy(url, (Array)a));
        transformers.put("blob", (url, a) -> new BlobProxy(url, (Blob)a));
        transformers.put("clob", (url, a) -> new ClobProxy(url, (Clob)a));
        transformers.put("nclob", (url, a) -> new ClobProxy(url, (NClob)a));
    }

    private static final Map<String, ThrowingTriConsumer<ResultSet, Integer, Object, SQLException>> updateByIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        updateByIndex.put("String", (rs, i, v) -> rs.updateString(i, (String)v));
        updateByIndex.put("byte", (rs, i, v) -> rs.updateByte(i, ((Number)v).byteValue()));
        updateByIndex.put("short", (rs, i, v) -> rs.updateShort(i, ((Number)v).shortValue()));
        updateByIndex.put("int", (rs, i, v) -> rs.updateInt(i, ((Number)v).intValue()));
        updateByIndex.put("long", (rs, i, v) -> rs.updateLong(i, ((Number)v).longValue()));
        updateByIndex.put("float", (rs, i, v) -> rs.updateFloat(i, ((Number)v).floatValue()));
        updateByIndex.put("double", (rs, i, v) -> rs.updateDouble(i, ((Number)v).doubleValue()));
        updateByIndex.put("boolean", (rs, i, v) -> rs.updateBoolean(i, (boolean)v));
        updateByIndex.put(Date.class.getSimpleName(), (rs, i, v) -> rs.updateDate(i, (Date)v));
        updateByIndex.put(Time.class.getSimpleName(), (rs, i, v) -> rs.updateTime(i, (Time)v));
        updateByIndex.put(Timestamp.class.getSimpleName(), (rs, i, v) -> rs.updateTimestamp(i, (Timestamp)v));
        // TODO: add clob, blob etc
    }

    private static final Map<String, ThrowingTriConsumer<ResultSet, String, Object, SQLException>> updateByLabel = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        updateByLabel.put("String", (rs, label, v) -> rs.updateString(label, (String)v));
        updateByLabel.put("byte", (rs, label, v) -> ((Number)v).byteValue());
        updateByLabel.put("short", (rs, label, v) -> rs.updateShort(label, ((Number)v).shortValue()));
        updateByLabel.put("int", (rs, label, v) -> rs.updateInt(label, ((Number)v).intValue()));
        updateByLabel.put("long", (rs, label, v) -> rs.updateLong(label, ((Number)v).longValue()));
        updateByLabel.put("float", (rs, label, v) -> rs.updateFloat(label, ((Number)v).floatValue()));
        updateByLabel.put("double", (rs, label, v) -> rs.updateDouble(label, ((Number)v).doubleValue()));
        updateByLabel.put("boolean", (rs, label, v) -> rs.updateBoolean(label, (boolean)v));
        updateByLabel.put(Date.class.getSimpleName(), (rs, label, v) -> rs.updateDate(label, (Date)v));
        updateByLabel.put(Time.class.getSimpleName(), (rs, label, v) -> rs.updateTime(label, (Time)v));
        updateByLabel.put(Timestamp.class.getSimpleName(), (rs, label, v) -> rs.updateTimestamp(label, (Timestamp)v));
        // TODO: add clob, blob etc
    }

    private static final Map<Integer, ThrowingBiFunction<ResultSet, Integer, ?, SQLException>> getterByType = new TreeMap<>();
    static {
        getterByType.put(Types.VARCHAR, ResultSet::getString);
        getterByType.put(Types.NVARCHAR, ResultSet::getNString);
        getterByType.put(Types.TINYINT, ResultSet::getByte);
        getterByType.put(Types.SMALLINT, ResultSet::getShort);
        getterByType.put(Types.INTEGER, ResultSet::getInt);
        getterByType.put(Types.BIGINT, ResultSet::getLong);
        getterByType.put(Types.BOOLEAN, ResultSet::getBoolean);
        getterByType.put(Types.BIT, ResultSet::getByte);
        getterByType.put(Types.FLOAT, ResultSet::getFloat);
        getterByType.put(Types.DOUBLE, ResultSet::getDouble);
        getterByType.put(Types.DATE, ResultSet::getDate);
        getterByType.put(Types.TIME, ResultSet::getTime);
        getterByType.put(Types.TIME_WITH_TIMEZONE, ResultSet::getTime);
        getterByType.put(Types.TIMESTAMP, ResultSet::getTimestamp);
        getterByType.put(Types.TIMESTAMP_WITH_TIMEZONE, ResultSet::getTimestamp);

        getterByType.put(Types.ARRAY, ResultSet::getArray);
        getterByType.put(Types.BLOB, ResultSet::getBlob);
        getterByType.put(Types.CLOB, ResultSet::getClob);
        getterByType.put(Types.NCLOB, ResultSet::getNClob);
        getterByType.put(Types.REF, ResultSet::getRef);

        getterByType.put(Types.JAVA_OBJECT, ResultSet::getObject);
    }


    private ResultSet getResultSet(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, prefix, id);
    }

    private <K, V, E extends RuntimeException> V getOrThrow(Map<K, V> map, K key, Function<K, E> exceptionFactory) {
        return Optional.ofNullable(map.get(key)).orElseThrow(() -> exceptionFactory.apply(key));
    }

    private Object[] readRow(ResultSet rs, String rsUrl) throws Exception {
        ResultSetMetaData md = rs.getMetaData();
        int n = md.getColumnCount();
        Object[] row = new Object[n];
        for (int i = 0; i < n; i++) {
            int column = i + 1;
            Object value;
            ThrowingBiFunction<ResultSet, Integer, ?, SQLException> getter = getterByType.get(md.getColumnType(column));
            if (getter != null) {
                try {
                    value = getter.apply(rs, column);
                } catch (SQLException e) {
                    value = getObjectOrException(rs, column);
                }
            } else {
                value = getObjectOrException(rs, column);
            }
            String type = md.getColumnTypeName(column);
            ThrowingBiFunction<String, Object, Object, Exception> transformer = transformers.get(type);
            row[i] = value == null || transformer == null ? value : entityToProxy(value, transformer, type.toLowerCase(), format("%s/%d", rsUrl, column));
        }
        return row;
    }

    private Object getObjectOrException(ResultSet rs, int column) {
        try {
            return rs.getObject(column);
        } catch (SQLException e) {
            return e;
        }
    }

    public RowData move(ResultSet rs, ThrowingFunction<ResultSet, Boolean, SQLException> move, String url) throws Exception {
        return move.apply(rs) ? new RowData(true, readRow(rs, parentUrl(url))) : new RowData(false, null);
    }
}

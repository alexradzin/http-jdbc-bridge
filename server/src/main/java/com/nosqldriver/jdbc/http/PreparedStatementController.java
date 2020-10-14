package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ParameterValue;
import com.nosqldriver.jdbc.http.model.ResultSetProxy;
import com.nosqldriver.jdbc.http.model.TransportableParameterMetaData;
import com.nosqldriver.jdbc.http.model.TransportableResultSetMetaData;
import com.nosqldriver.util.function.ThrowingTriConsumer;
import spark.Request;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.TreeMap;

import static java.lang.String.format;
import static spark.Spark.get;
import static spark.Spark.put;

public class PreparedStatementController extends StatementController {
    private static final String baseUrl = "/connection/:connection/prepared-statement/:statement";
    protected PreparedStatementController(Map<String, Object> attributes, ObjectMapper objectMapper) {
        super(attributes, objectMapper, baseUrl);
        get(format("%s/query", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), PreparedStatement::executeQuery, ResultSetProxy::new, "resultset", req.url(), getToken(req)));
        get(format("%s/update", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), PreparedStatement::executeUpdate, ResultSetProxy::new, "resultset", req.url(), getToken(req)));
        get(format("%s/execute", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), PreparedStatement::execute, ResultSetProxy::new, "resultset", req.url(), getToken(req)));

        get(format("%s/:resultset/metadata", baseUrl), JSON, (req, res) -> retrieve2(() -> getStatement(attributes, req), PreparedStatement::getMetaData, TransportableResultSetMetaData::new, "metadata", req.url(), getToken(req)));
        get(format("%s/:resultset/parametermetadata", baseUrl), JSON, (req, res) -> retrieve2(() -> getStatement(attributes, req), PreparedStatement::getParameterMetaData, TransportableParameterMetaData::new, "metadata", req.url(), getToken(req)));

        put(baseUrl, JSON, (req, res) -> accept(() -> getStatement(attributes, req), rs -> {
            ParameterValue<?, ?> parameterValue = objectMapper.readValue(req.body(), ParameterValue.class);
            int index = parameterValue.getIndex();
            String typeName = parameterValue.getTypeName();
            setByIndex.get(typeName).accept(rs, index, parameterValue.getValue());
        }));
    }

    private PreparedStatement getStatement(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "prepared-statement", ":statement");
    }


    private static final Map<String, ThrowingTriConsumer<PreparedStatement, Integer, Object, SQLException>> setByIndex = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    static {
        setByIndex.put("String", (ps, i, v) -> ps.setString(i, (String)v));
        setByIndex.put("byte", (ps, i, v) -> ps.setByte(i, (byte)v));
        setByIndex.put("short", (ps, i, v) -> ps.setShort(i, (short)v));
        setByIndex.put("int", (ps, i, v) -> ps.setInt(i, (int)v));
        setByIndex.put("long", (ps, i, v) -> ps.setLong(i, (long)v));
        setByIndex.put("float", (ps, i, v) -> ps.setFloat(i, (float)v));
        setByIndex.put("double", (ps, i, v) -> ps.setDouble(i, (double)v));
        setByIndex.put("boolean", (ps, i, v) -> ps.setBoolean(i, (boolean)v));
        setByIndex.put(Date.class.getSimpleName(), (rs, i, v) -> rs.setDate(i, (Date)v));
        setByIndex.put(Time.class.getSimpleName(), (rs, i, v) -> rs.setTime(i, (Time)v));
        setByIndex.put(Timestamp.class.getSimpleName(), (rs, i, v) -> rs.setTimestamp(i, (Timestamp)v));
        setByIndex.put(Blob.class.getSimpleName(), (rs, i, v) -> rs.setBlob(i, (Blob)v));
        setByIndex.put(Clob.class.getSimpleName(), (rs, i, v) -> rs.setClob(i, (Clob)v));
        setByIndex.put(NClob.class.getSimpleName(), (rs, i, v) -> rs.setClob(i, (NClob)v));
        // TODO: add clob, blob with stream arguments; set streams etc
    }
}

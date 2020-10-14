package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ArrayProxy;
import com.nosqldriver.jdbc.http.model.BlobProxy;
import com.nosqldriver.jdbc.http.model.CallableStatementProxy;
import com.nosqldriver.jdbc.http.model.ClobProxy;
import com.nosqldriver.jdbc.http.model.PreparedStatementProxy;
import com.nosqldriver.jdbc.http.model.SQLXMLProxy;
import com.nosqldriver.jdbc.http.model.StatementProxy;
import com.nosqldriver.jdbc.http.model.StructProxy;
import com.nosqldriver.jdbc.http.model.TransportableDatabaseMetaData;
import com.nosqldriver.jdbc.http.model.TransportableSQLWarning;
import com.nosqldriver.jdbc.http.model.TransportableSavepoint;
import spark.Request;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static com.nosqldriver.jdbc.http.Util.decode;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;

public class ConnectionController extends BaseController {
    private final Executor executor = Executors.newSingleThreadExecutor();

    public ConnectionController(Map<String, Object> attributes, ObjectMapper objectMapper) {
        super(attributes, objectMapper);

        post("/connection/:connection/statement", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> {
            Integer type = intArg(req, "type");
            Integer concurrency = intArg(req, "concurrency");
            Integer holdability = intArg(req, "holdability");

            if (type != null && concurrency != null && holdability != null) {
                return connection.createStatement(type, concurrency, holdability);
            }
            if (type != null && concurrency != null) {
                return connection.createStatement(type, concurrency);
            }
            return connection.createStatement();
        },
        StatementProxy::new, "statement", req.url(), getToken(req)));

        post("/connection/:connection/prepared-statement", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> {
            String sql = objectMapper.readValue(req.body(), String.class);
            Integer type = intArg(req, "type");
            Integer concurrency = intArg(req, "concurrency");
            Integer holdability = intArg(req, "holdability");
            Integer autoGeneratedKeys = intArg(req, "keys");
            String[] columnNames = stringArrayArg(req, "names");
            int[] columnIndexes = intArrayArg(req, "indexes");

            if (type != null && concurrency != null && holdability != null) {
                return connection.prepareStatement(sql, type, concurrency, holdability);
            }
            if (type != null && concurrency != null) {
                return connection.prepareStatement(sql, type, concurrency);
            }
            if (autoGeneratedKeys != null) {
                return connection.prepareStatement(sql, autoGeneratedKeys);
            }
            if (columnNames != null) {
                return connection.prepareStatement(sql, columnNames);
            }
            if (columnIndexes != null) {
                return connection.prepareStatement(sql, columnIndexes);
            }
            return connection.prepareStatement(sql);
        }, PreparedStatementProxy::new, "prepared-statement", req.url(), getToken(req)));

        post("/connection/:connection/callable-statement", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> {
            String sql = objectMapper.readValue(req.body(), String.class);
            Integer type = intArg(req, "type");
            Integer concurrency = intArg(req, "concurrency");
            Integer holdability = intArg(req, "holdability");

            if (type != null && concurrency != null && holdability != null) {
                return connection.prepareCall(sql, type, concurrency, holdability);
            }
            if (type != null && concurrency != null) {
                return connection.prepareCall(sql, type, concurrency);
            }
            return connection.prepareCall(sql);
        }, CallableStatementProxy::new, "callable-statement", req.url(), getToken(req)));

        post("/connection/:connection/nativesql", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> connection.nativeSQL(req.body())));

        post("/connection/:connection/autocommit", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setAutoCommit(Boolean.parseBoolean(req.body()))));
        get("/connection/:connection/autocommit", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getAutoCommit));

        post("/connection/:connection/commit", JSON, (req, res) -> accept(() -> getConnection(attributes, req), Connection::commit));
        post("/connection/:connection/rollback", JSON, (req, res) -> accept(() -> getConnection(attributes, req), Connection::rollback));
        delete("/connection/:connection", JSON, (req, res) -> accept(() -> getConnection(attributes, req), Connection::close));
        get("/connection/:connection/closed", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::isClosed));

        get("/connection/:connection/metadata", JSON, (req, res) -> retrieve2(() -> getConnection(attributes, req), Connection::getMetaData, TransportableDatabaseMetaData::new, "metadata", req.url(), getToken(req)));

        post("/connection/:connection/readonly", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setReadOnly(Boolean.parseBoolean(req.body()))));
        get("/connection/:connection/readonly", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::isReadOnly));

        post("/connection/:connection/catalog", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setCatalog(req.body())));
        get("/connection/:connection/catalog", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getCatalog));

        post("/connection/:connection/transaction/isolation", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setTransactionIsolation(Integer.parseInt(req.body()))));
        get("/connection/:connection/transaction/isolation", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getTransactionIsolation));

        get("/connection/:connection/warnings", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getWarnings, TransportableSQLWarning::new));
        delete("/connection/:connection/warnings", JSON, (req, res) -> accept(() -> getConnection(attributes, req), Connection::clearWarnings));

        post("/connection/:connection/typemap", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setTypeMap(objectMapper.readValue(req.body(), HashMap.class))));
        get("/connection/:connection/typemap", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getTypeMap));

        post("/connection/:connection/holdability", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setHoldability(Integer.parseInt(req.body()))));
        get("/connection/:connection/holdability", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getHoldability));

        post("/connection/:connection/savepoint", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> req.body() == null ? connection.setSavepoint() : connection.setSavepoint(req.body()), TransportableSavepoint::new));
        delete("/connection/:connection/savepoint", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.releaseSavepoint(objectMapper.readValue(req.body(), TransportableSavepoint.class))));
        post("/connection/:connection/rollback", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> req.body() == null ? connection.setSavepoint() : connection.setSavepoint(req.body()), TransportableSavepoint::new));

        post("/connection/:connection/clob", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::createClob, ClobProxy::new, "clob", req.url(), getToken(req)));
        post("/connection/:connection/nclob", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::createNClob, ClobProxy::new, "nclob", req.url(), getToken(req)));
        post("/connection/:connection/blob", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::createBlob, BlobProxy::new, "blob", req.url(), getToken(req)));
        post("/connection/:connection/sqlxml", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::createSQLXML, SQLXMLProxy::new, "sqlxml", req.url(), getToken(req)));
        post("/connection/:connection/array/:type", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> connection.createArrayOf(req.params(":type"), objectMapper.readValue(req.body(), Object[].class)), ArrayProxy::new, "array", req.url(), getToken(req)));
        post("/connection/:connection/struct/:type", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> connection.createStruct(req.params(":type"), objectMapper.readValue(req.body(), Object[].class)), StructProxy::new, "struct", req.url(), getToken(req)));

        get("/connection/:connection/valid/:timeout", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> connection.isValid(intParam(req, ":timeout"))));

        get("/connection/:connection/client/info/:name", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> connection.getClientInfo(decode(req.params(":name")))));
        get("/connection/:connection/client/info/", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), connection -> connection.getClientInfo("")));
        get("/connection/:connection/client/info", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getClientInfo));
        post("/connection/:connection/client/info/:name", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setClientInfo(decode(req.params(":name")), req.body())));
        post("/connection/:connection/client/info", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setClientInfo(objectMapper.readValue(req.body(), Properties.class))));

        post("/connection/:connection/schema", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setSchema(req.body())));
        get("/connection/:connection/schema", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getSchema));

        post("/connection/:connection/networktimeout", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.setNetworkTimeout(executor, Integer.parseInt(req.body()))));
        get("/connection/:connection/networktimeout", JSON, (req, res) -> retrieve(() -> getConnection(attributes, req), Connection::getNetworkTimeout));
        post("/connection/:connection/abort", JSON, (req, res) -> accept(() -> getConnection(attributes, req), connection -> connection.abort(executor)));

        new DatabaseMetaDataController(attributes, objectMapper);
        new StatementController(attributes, objectMapper, "/connection/:connection/statement/:statement");
        new PreparedStatementController(attributes, objectMapper);
    }

    private Connection getConnection(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "connection", ":connection");
    }
}

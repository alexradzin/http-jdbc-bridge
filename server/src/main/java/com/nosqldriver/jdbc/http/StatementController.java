package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ResultSetProxy;
import com.nosqldriver.jdbc.http.model.TransportableSQLWarning;
import com.nosqldriver.util.function.ThrowingBiFunction;
import com.nosqldriver.util.function.ThrowingFunction;
import spark.Request;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static java.lang.String.format;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.post;
import static spark.Spark.put;

public class StatementController extends BaseController {
    private final String prefix;
    protected StatementController(Map<String, Object> attributes, ObjectMapper objectMapper, String baseUrl) {
        super(attributes, objectMapper);
        String[] urlParts = baseUrl.split("/");
        prefix = urlParts[urlParts.length - 2];


        post(format("%s/query", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> statement.executeQuery(objectMapper.readValue(req.body(), String.class)), ResultSetProxy::new, "resultset", req.url(), getToken(req)));

        //post(format("%s/query", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> statement.executeQuery(objectMapper.readValue(req.body(), String.class)), (u, t) -> authorize(req, "", new ResultSetProxy(u, t)), "resultset", req.url(), getToken(req)));


        post(format("%s/execute", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> exec(req, statement::execute, statement::execute, statement::execute, statement::execute)));
        post(format("%s/update", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> exec(req, statement::executeUpdate, statement::executeUpdate, statement::executeUpdate, statement::executeUpdate)));
        post(format("%s/large/update", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> exec(req, statement::executeLargeUpdate, statement::executeLargeUpdate, statement::executeLargeUpdate, statement::executeLargeUpdate)));

        delete(format("%s", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), Statement::close));
        delete(format("%s/cancel", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), Statement::cancel));

        post(format("%s/maxfieldsize", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setMaxFieldSize(Integer.parseInt(req.body()))));
        get(format("%s/maxfieldsize", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getMaxFieldSize));

        post(format("%s/maxrows", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setMaxRows(Integer.parseInt(req.body()))));
        get(format("%s/maxrows", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getMaxRows));

        post(format("%s/large/maxrows", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setLargeMaxRows(Long.parseLong(req.body()))));
        get(format("%s/large/maxrows", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getLargeMaxRows));

        post(format("%s/escapeprocessing", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setEscapeProcessing(Boolean.parseBoolean(req.body()))));

        post(format("%s/querytimeout", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setQueryTimeout(Integer.parseInt(req.body()))));
        get(format("%s/querytimeout", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getQueryTimeout));

        get(format("%s/warnings", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getWarnings, TransportableSQLWarning::new));
        delete(format("%s/warnings", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), Statement::clearWarnings));

        post(format("%s/cursorname", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setCursorName(req.body())));

        get(format("%s/resultset", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getResultSet, ResultSetProxy::new, "resultset", req.url(), getToken(req)));

        get(format("%s/updatecount", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getUpdateCount));
        get(format("%s/large/updatecount", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getLargeUpdateCount));
        get(format("%s/more", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), s -> {
            Integer current = intArg(req, "current");
            return current == null ? s.getMoreResults() : s.getMoreResults(current);
        }));

        post(format("%s/fetch/direction", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setFetchDirection(Integer.parseInt(req.body()))));
        get(format("%s/fetch/direction", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getFetchDirection));

        post(format("%s/fetch/size", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setFetchSize(Integer.parseInt(req.body()))));
        get(format("%s/fetch/size", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getFetchSize));

        get(format("%s/resultset/concurrency", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getResultSetConcurrency));
        get(format("%s/resultset/type", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getResultSetType));
        get(format("%s/resultset/holdability", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getResultSetHoldability));

        put(format("%s/batch", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.addBatch(objectMapper.readValue(req.body(), String.class))));
        post(format("%s/batch", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), Statement::executeBatch));
        post(format("%s/large/batch", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), Statement::executeLargeBatch));
        delete(format("%s/batch", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), Statement::clearBatch));

        get(format("%s/generatedkeys", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::getGeneratedKeys, ResultSetProxy::new, "resultset", req.url(), getToken(req)));
        get(format("%s/closed", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::isClosed));

        post(format("%s/closeoncompletion", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), Statement::closeOnCompletion));
        get(format("%s/closeoncompletion", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::isCloseOnCompletion));

        post(format("%s/poolable", baseUrl), JSON, (req, res) -> accept(() -> getStatement(attributes, req), statement -> statement.setPoolable(Boolean.parseBoolean(req.body()))));
        get(format("%s/poolable", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), Statement::isPoolable));

        post(format("%s/enquote/literal", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> statement.enquoteLiteral(req.body())));
        post(format("%s/enquote/identifier/:always", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> statement.enquoteIdentifier(req.body(), Boolean.parseBoolean(req.params("always")))));
        get(format("%s/simple/identifier/:identifier", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> statement.isSimpleIdentifier(stringParam(req, ":identifier"))));
        post(format("%s/enquote/nchar/literal", baseUrl), JSON, (req, res) -> retrieve(() -> getStatement(attributes, req), statement -> statement.enquoteNCharLiteral(req.body())));

        new ResultSetController(attributes, objectMapper, baseUrl + "/resultset/:resultset");
    }

    private Statement getStatement(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, prefix, ":statement");
    }

    private <T> T exec(
            Request req,
            ThrowingFunction<String, T, SQLException> exec,
            ThrowingBiFunction<String, Integer, T, SQLException> execAutoGenKeys,
            ThrowingBiFunction<String, int[], T, SQLException> execIndexes,
            ThrowingBiFunction<String, String[], T, SQLException> execNames) throws JsonProcessingException, SQLException {
        String sql = objectMapper.readValue(req.body(), String.class);
        Integer autoGeneratedKeys = intArg(req, "keys");
        String[] columnNames = stringArrayArg(req, "names");
        int[] columnIndexes = intArrayArg(req, "indexes");

        if (autoGeneratedKeys != null) {
            return execAutoGenKeys.apply(sql, autoGeneratedKeys);
        }
        if (columnIndexes != null) {
            return execIndexes.apply(sql, columnIndexes);
        }
        if (columnNames != null) {
            return execNames.apply(sql, columnNames);
        }
        return exec.apply(sql);
    }
}

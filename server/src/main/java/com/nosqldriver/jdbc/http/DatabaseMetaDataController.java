package com.nosqldriver.jdbc.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nosqldriver.jdbc.http.model.ResultSetProxy;
import spark.Request;

import java.sql.DatabaseMetaData;
import java.util.Map;
import java.util.stream.Stream;

import static java.lang.String.format;
import static spark.Spark.get;

public class DatabaseMetaDataController extends BaseController {
    protected DatabaseMetaDataController(Map<String, Object> attributes, ObjectMapper objectMapper) {
        super(attributes, objectMapper);
        String baseUrl = "/connection/:connection/metadata/:metadata";

        get(format("%s/catalogs", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), DatabaseMetaData::getCatalogs, ResultSetProxy::new, "schemas", req.url()));
        get(format("%s/table/types", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), DatabaseMetaData::getTableTypes, ResultSetProxy::new, "types", req.url()));
        get(format("%s/type/info", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), DatabaseMetaData::getTypeInfo, ResultSetProxy::new, "info", req.url()));
        get(format("%s/schemas", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getSchemas(stringArg(req, "catalog"), stringArg(req, "schema")), ResultSetProxy::new, "schemas", req.url()));
        get(format("%s/functions", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getFunctions(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "function")), ResultSetProxy::new, "functions", req.url()));
        get(format("%s/function/columns", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getFunctionColumns(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "function"), stringArg(req, "column")), ResultSetProxy::new, "columns", req.url()));
        get(format("%s/pseudo/columns", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getPseudoColumns(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table"), stringArg(req, "column")), ResultSetProxy::new, "columns", req.url()));

        get(format("%s/supports/transaction/isolation/level/:level", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.supportsTransactionIsolationLevel(intParam(req, ":level"))));
        get(format("%s/supports/resultset/type/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.supportsResultSetType(intParam(req, ":type"))));
        get(format("%s/own/updates/visible/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.ownUpdatesAreVisible(intParam(req, ":type"))));
        get(format("%s/own/deletes/visible/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.ownDeletesAreVisible(intParam(req, ":type"))));
        get(format("%s/own/inserts/visible/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.ownInsertsAreVisible(intParam(req, ":type"))));
        get(format("%s/others/updates/visible/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.othersUpdatesAreVisible(intParam(req, ":type"))));
        get(format("%s/others/deletes/visible/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.othersDeletesAreVisible(intParam(req, ":type"))));
        get(format("%s/others/inserts/visible/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.othersInsertsAreVisible(intParam(req, ":type"))));
        get(format("%s/updates/detected/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.updatesAreDetected(intParam(req, ":type"))));
        get(format("%s/deletes/detected/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.deletesAreDetected(intParam(req, ":type"))));
        get(format("%s/inserts/detected/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.insertsAreDetected(intParam(req, ":type"))));
        get(format("%s/supports/resultset/holdability/:holdability", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.supportsResultSetHoldability(intParam(req, ":holdability"))));
        get(format("%s/supports/resultset/type/:type", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.supportsResultSetType(intParam(req, ":type"))));

        get(format("%s/supports/convert/:from/:to", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.supportsConvert(intParam(req, ":from"), intParam(req, ":to"))));
        get(format("%s/supports/resultset/concurrency/:type/:concurrency", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.supportsResultSetConcurrency(intParam(req, ":type"), intParam(req, ":concurrency"))));

        get(format("%s/procedures", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getProcedures(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "procedures")), ResultSetProxy::new, "procedures", req.url()));
        get(format("%s/procedure/columns", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getProcedureColumns(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "procedure"), stringArg(req, "catalog")), ResultSetProxy::new, "columns", req.url()));
        get(format("%s/tables", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getTables(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table"), stringArrayArg(req, "types")), ResultSetProxy::new, "tables", req.url()));
        get(format("%s/table/privileges", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getTablePrivileges(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table")), ResultSetProxy::new, "privileges", req.url()));

        get(format("%s/version/columns", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getVersionColumns(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table")), ResultSetProxy::new, "columns", req.url()));
        get(format("%s/primary/keys", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getPrimaryKeys(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table")), ResultSetProxy::new, "keys", req.url()));
        get(format("%s/imported/keys", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getImportedKeys(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table")), ResultSetProxy::new, "keys", req.url()));
        get(format("%s/exported/keys", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getExportedKeys(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table")), ResultSetProxy::new, "keys", req.url()));

        get(format("%s/columns", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getColumns(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table"), stringArg(req, "column")), ResultSetProxy::new, "columns", req.url()));
        get(format("%s/column/privileges", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getColumnPrivileges(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table"), stringArg(req, "column")), ResultSetProxy::new, "privileges", req.url()));
        get(format("%s/best/row/identifier", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req),
                md -> {
                    return md.getBestRowIdentifier(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table"), intArg(req, "scope"), Boolean.parseBoolean(req.queryParams("nullable")));
                },
                ResultSetProxy::new, "identifier", req.url()));
        get(format("%s/crossreference", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getCrossReference(
                stringArg(req, "parentCatalog"), stringArg(req, "parentSchema"), stringArg(req, "parentTable"),
                stringArg(req, "foreignCatalog"), stringArg(req, "foreignSchema"), stringArg(req, "foreignTable")
        ), ResultSetProxy::new, "crossreference", req.url()));

        get(format("%s/index/info", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getIndexInfo(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "table"), Boolean.parseBoolean(req.params("unique")), Boolean.parseBoolean(req.queryParams("approximate"))), ResultSetProxy::new, "info", req.url()));
        get(format("%s/udts", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getUDTs(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "typename"), intArrayArg(req, "types")), ResultSetProxy::new, "udts", req.url()));

        get(format("%s/super/types", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getSuperTypes(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "typename")), ResultSetProxy::new, "types", req.url()));
        get(format("%s/super/tables", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getSuperTables(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "typename")), ResultSetProxy::new, "tables", req.url()));
        get(format("%s/attributes", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), md -> md.getAttributes(stringArg(req, "catalog"), stringArg(req, "schema"), stringArg(req, "typename"), stringArg(req, "attribute")), ResultSetProxy::new, "attributes", req.url()));

        get(format("%s/rowidlifetime", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), DatabaseMetaData::getRowIdLifetime));
        get(format("%s/generatedkeyalwaysreturned", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), DatabaseMetaData::generatedKeyAlwaysReturned));
        //get(format("%s/supports/sharding", baseUrl), JSON, (req, res) -> retrieve(() -> getMetadata(attributes, req), DatabaseMetaData::supportsSharding));

        String subUrl = "/connection/:connection/metadata/:metadata/%s";
        Stream.of("schemas/:schemas", "tables/:tables", "udts/:udts", "columns/:columns", "attributes/:attributes",
                "table/types/:types", "type/info/:info", "index/info/:info",
                "procedures/:procedures", "table/privileges/:privileges",
                "version/columns/:columns", "primary/keys/:keys", "imported/keys/:keys", "exported/keys/:keys", "super/types/:types",
                "super/tables/:tables", "functions/:functions",
                "procedure/columns/:columns", "function/columns/:columns", "pseudo/columns/:columns",
                "column/privileges/:privileges", "crossreference/:crossreference", "best/row/identifier/:identifier")
                .forEach(entity -> new ResultSetController(attributes, objectMapper, format(subUrl, entity)));
    }

    private DatabaseMetaData getMetadata(Map<String, Object> attributes, Request req) {
        return getEntity(attributes, req, "metadata", ":metadata");
    }


}

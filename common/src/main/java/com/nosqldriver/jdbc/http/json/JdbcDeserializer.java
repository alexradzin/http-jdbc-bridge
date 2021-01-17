package com.nosqldriver.jdbc.http.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.nosqldriver.jdbc.http.model.ArrayProxy;
import com.nosqldriver.jdbc.http.model.BlobProxy;
import com.nosqldriver.jdbc.http.model.CallableStatementProxy;
import com.nosqldriver.jdbc.http.model.ClobProxy;
import com.nosqldriver.jdbc.http.model.ConnectionProxy;
import com.nosqldriver.jdbc.http.model.PreparedStatementProxy;
import com.nosqldriver.jdbc.http.model.ResultSetProxy;
import com.nosqldriver.jdbc.http.model.SQLXMLProxy;
import com.nosqldriver.jdbc.http.model.StatementProxy;
import com.nosqldriver.jdbc.http.model.StructProxy;
import com.nosqldriver.jdbc.http.model.TransportableDatabaseMetaData;
import com.nosqldriver.jdbc.http.model.TransportableParameterMetaData;
import com.nosqldriver.jdbc.http.model.TransportableRef;
import com.nosqldriver.jdbc.http.model.TransportableResultSetMetaData;
import com.nosqldriver.jdbc.http.model.TransportableSavepoint;

import java.io.IOException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JdbcDeserializer extends StdDeserializer<Object> {
    private static final Map<String, Function<JsonNode, Object>> creators = Stream.of(
            new SimpleEntry<>((String)null, (Function<JsonNode, Object>) jsonNode -> jsonNode),
            new SimpleEntry<>(java.sql.Date.class.getName(), (Function<JsonNode, Object>) jsonNode -> new java.sql.Date(jsonNode.get("epoch").longValue())),
            new SimpleEntry<>(Timestamp.class.getName(), (Function<JsonNode, Object>) jsonNode -> new Timestamp(jsonNode.get("epoch").longValue())),
            new SimpleEntry<>(Time.class.getName(), (Function<JsonNode, Object>) jsonNode -> Time.valueOf((jsonNode.get("time")).asText()))
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    private static final Function<JsonNode, Object> proxyFactory = new Function<>() {
        private final Map<String, Class<?>> proxyClasses = Stream.of(
                new SimpleEntry<>(Array.class.getName(), ArrayProxy.class),
                new SimpleEntry<>(SQLXML.class.getName(), SQLXMLProxy.class),
                new SimpleEntry<>(Struct.class.getName(), StructProxy.class),
                new SimpleEntry<>(Clob.class.getName(), ClobProxy.class),
                new SimpleEntry<>(NClob.class.getName(), ClobProxy.class),
                new SimpleEntry<>(Blob.class.getName(), BlobProxy.class),
                new SimpleEntry<>(Connection.class.getName(), ConnectionProxy.class),
                new SimpleEntry<>(Statement.class.getName(), StatementProxy.class),
                new SimpleEntry<>(PreparedStatement.class.getName(), PreparedStatementProxy.class),
                new SimpleEntry<>(CallableStatement.class.getName(), CallableStatementProxy.class),
                new SimpleEntry<>(DatabaseMetaData.class.getName(), TransportableDatabaseMetaData.class),
                new SimpleEntry<>(ResultSetMetaData.class.getName(), TransportableResultSetMetaData.class),
                new SimpleEntry<>(ParameterMetaData.class.getName(), TransportableParameterMetaData.class),
                new SimpleEntry<>(ResultSet.class.getName(), ResultSetProxy.class),
                new SimpleEntry<>(Savepoint.class.getName(), TransportableSavepoint.class),
                new SimpleEntry<>(Ref.class.getName(), TransportableRef.class)
        ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

        @Override
        public Object apply(JsonNode jsonNode) {
            //TODO: add validations and throw exceptions if this method was called not for proxy; otherwise NPEs etc. are expected
            String entityUrl = jsonNode.get("entityUrl").asText();
            try {
                return proxyClasses.get(jsonNode.get("clazz").asText()).getConstructor(String.class).newInstance(entityUrl);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        }
    };


    private static final Map<JsonNodeType, Function<JsonNode, Object>> nodeValueGetter = Stream.of(
            //TODO: add array?
            //new SimpleEntry<>(JsonNodeType.ARRAY, (Function<JsonNode, Object>) node -> node.elements()),
            new SimpleEntry<>(JsonNodeType.NUMBER, new Function<JsonNode, Object>() {
                @Override
                public Object apply(JsonNode node) {
                    if (node.isInt()) {
                        return node.asInt();
                    }
                    return node.asDouble();
                }
            }),
            new SimpleEntry<>(JsonNodeType.STRING, (Function<JsonNode, Object>) JsonNode::asText),
            new SimpleEntry<>(JsonNodeType.BOOLEAN, (Function<JsonNode, Object>) JsonNode::asBoolean),
            new SimpleEntry<>(JsonNodeType.OBJECT, (Function<JsonNode, Object>) node -> creators.getOrDefault(text(node.get("clazz")), proxyFactory).apply(node))
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));

    public JdbcDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonNode node = jp.getCodec().readTree(jp);
        return nodeValueGetter.get(node.getNodeType()).apply(node);
    }

    private static String text(JsonNode node) {
        return node == null ? null : node.asText();
    }
}

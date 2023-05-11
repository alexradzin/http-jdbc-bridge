package com.nosqldriver.jdbc.http.permissions;

import com.nosqldriver.util.function.ThrowingFunction;
import net.sf.jsqlparser.JSQLParserException;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class StatementPermissionsValidator implements ThrowingFunction<String, String, SQLException> {
    // TODO: automatically receive this map from PermissionsParser
    private static final Map<Class<?>, List<String>> queryParts = Map.of(
            Select.class, List.of("fields", "where", "orderBy", "limit", "innerJoin", "outerJoin", "groupBy"),
            Insert.class, List.of("where", "fields", "limit"),
            Update.class, List.of("where", "fields"),
            Delete.class, List.of("where"),
            Truncate.class, List.of(),
            Create.class, List.of("on", "from"),
            Drop.class, List.of("type"),
            Alter.class, List.of("type")
    );
    private final Map<Class<?>, List<StatementPermission<?>>> statementsPermissions = new ConcurrentHashMap<>();
    private final PermissionsParser permissionsParser = new PermissionsParser();
    private final SqlParser sqlParser = new SqlParser();

    public StatementPermissionsValidator addConfiguration(Map<Class<?>, List<StatementPermission<?>>> add) {
        add.forEach((key, value) -> statementsPermissions.merge(key, value, (one, two) -> Stream.concat(one.stream(), two.stream()).collect(toList())));
        return this;
    }

    public StatementPermissionsValidator addConfiguration(InputStream in) throws IOException {
        return addConfiguration(permissionsParser.parseConfiguration(in));
    }

    public void clean() {
        statementsPermissions.clear();
    }

    public String validate(String queryStr) throws ParseException, SQLException {
        Object query = parseQuery(queryStr);
        Class<?> queryType = query == null ? Object.class : query.getClass();
        List<StatementPermission<?>> permissions = statementsPermissions.getOrDefault(queryType, List.of());
        List<String> elements = queryParts.getOrDefault(queryType, List.of());
        validate(permissions, elements, query);
        return queryStr;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private boolean validate(List<StatementPermission<?>> permissions, List<String> elements, Object query) throws SQLException {
        return new StatementPermissions(permissions, elements).validate(query);
    }

    private Object parseQuery(String queryStr) throws ParseException {
        try {
            return sqlParser.parse(queryStr);
        } catch (JSQLParserException e) {
            throw new ParseException(e.getMessage(), 0);
        }
    }

    @Override
    public String apply(String query) throws SQLException {
        try {
            return validate(query);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }
}

package com.nosqldriver.jdbc.http.permissions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.compile;
import static java.util.stream.Collectors.toList;

public class PermissionsParser {
    private static final List<Map.Entry<Pattern, Function<Matcher, Map.Entry<Class<?>, StatementPermission<?>>>>> statementParsers = List.of(
            entry(
                    compile("select (.+) from (\\S+)", CASE_INSENSITIVE),
                    m -> entry(
                            Select.class,
                            new StatementPermission<>(
                                    Map.of("fields", new EnumeratedListPermission<>(Select::getFields, "Fields %s cannot be queried", List.of(m.group(1).split("\\s*,\\s*")))),
                                    List.of(m.group(2).split("\\s*,\\s*")),
                                    Select::getTable)
                    )
            ),

            entry(
                    compile("select (.+) from (\\S+).*?(?:\\s+limit\\s+(\\d+))", CASE_INSENSITIVE),
                    m -> entry(
                            Select.class,
                            new StatementPermission<>(
                                    Map.of("limit", new SelectLimitPermission<>(Select::getLimit, Integer.parseInt(m.group(3)))),
                                    List.of(m.group(2).split("\\s*,\\s*")),
                                    Select::getTable)
                    )
            ),

            entry(
                    compile("select (.+) from (\\S+).*?(?:\\s+order\\s+by\\s+(\\S+))", CASE_INSENSITIVE),
                    m -> entry(
                            Select.class,
                            new StatementPermission<>(
                                    Map.of("orderBy", new EnumeratedListPermission<>(Select::getOrderBy, "Cannot order by %s", List.of(m.group(3).split("\\s*,\\s*")))),
                                    List.of(m.group(2).split("\\s*,\\s*")),
                                    Select::getTable)
                    )
            ),

            entry(
                    compile("select (.+) from (\\S+).*?(?:\\s+group\\s+by\\s+(\\S+))", CASE_INSENSITIVE),
                    m -> entry(
                            Select.class,
                            new StatementPermission<>(
                                    Map.of("groupBy", new EnumeratedListPermission<>(Select::getGroupBy, "Cannot group by %s", List.of(m.group(3).split("\\s*,\\s*")))),
                                    List.of(m.group(2).split("\\s*,\\s*")),
                                    Select::getTable)
                    )
            ),


            entry(
                    compile("select (.+) from (\\S+).*(?:\\s+inner)?(?!left|outer)\\s+join\\s+(\\S+)", CASE_INSENSITIVE),
                    m -> entry(
                            Select.class,
                            new StatementPermission<>(
                                    Map.of("innerJoin", new EnumeratedListPermission<>(Select::getInnerJoins, "Cannot join with %s", List.of(m.group(3).split("\\s*,\\s*")))),
                                    List.of(m.group(2).split("\\s*,\\s*")),
                                    Select::getTable)
                    )
            ),

            entry(
                    compile("select (.+) from (\\S+).*?(?:\\s+(?:left|outer|left\\s+outer)\\s+join\\s+(\\S+))", CASE_INSENSITIVE),
                    m -> entry(
                            Select.class,
                            new StatementPermission<>(
                                    Map.of("outerJoin", new EnumeratedListPermission<>(Select::getOuterJoins, "Cannot join with %s", List.of(m.group(3).split("\\s*,\\s*")))),
                                    List.of(m.group(2).split("\\s*,\\s*")),
                                    Select::getTable)
                    )
            ),

            entry(
                    compile("select (.+) from (\\S+).*?(?:\\s+where\\s+(.*))", CASE_INSENSITIVE),
                    m -> entry(
                            Select.class,
                            new StatementPermission<>(
                                    Map.of("where", new WherePermission<>(Select::getWhere, parseComparisonPermissions(m.group(3)))),
                                    List.of(m.group(2).split("\\s*,\\s*")),
                                    Select::getTable)
                    )
            ),

            entry(
                    compile("insert\\s+into\\s+(\\S+)\\s+\\(\\s*(.+?)\\s*\\)(?:\\s+limit\\s*([0-9*]+))?", CASE_INSENSITIVE),
                    m -> entry(
                            Insert.class,
                            new StatementPermission<>(
                                    Map.of(
                                            "fields", new EnumeratedListPermission<>(Insert::getFields, "Fields %s cannot be inserted", List.of(m.group(2).split("\\s*,\\s*"))),
                                            "limit", m.groupCount() > 2 && m.group(3) != null && m.group(3).matches("\\d+") ? new SelectLimitPermission<>(Insert::getLimit, Integer.parseInt(m.group(3))) : x -> null
                                    ),
                                    List.of(m.group(1).split("\\s*,\\s*")),
                                    Insert::getTable)
                    )
            ),

            entry(
                    compile("update\\s+(\\S+)(?:\\s+set\\s+\\((.+?)\\))?(?:\\s+where\\s+(.+))?", CASE_INSENSITIVE),
                    m -> entry(
                            Update.class,
                            new StatementPermission<>(
                                    Map.of(
                                            "fields", m.group(2) != null ? new EnumeratedListPermission<>(Update::getFields, "Fields %s cannot be inserted", List.of(m.group(2).split("\\s*,\\s*"))) : x -> null,
                                            "where", m.group(3) != null ? new WherePermission<>(Update::getWhere, parseComparisonPermissions(m.group(3))) : x -> null
                                    ),
                                    List.of(m.group(1).split("\\s*,\\s*")),
                                    Update::getTable)
                    )
            ),

            entry(
                    compile("delete\\s+from\\s+(\\S+)(?:\\s+where\\s+(.+))?", CASE_INSENSITIVE),
                    m -> entry(
                            Delete.class,
                            new StatementPermission<>(
                                    Map.of(
                                            "where", m.group(2) != null ? new WherePermission<>(Delete::getWhere, parseComparisonPermissions(m.group(2))) : x -> null
                                    ),
                                    List.of(m.group(1).split("\\s*,\\s*")),
                                    Delete::getTable)
                    )
            )
    );

    public Map<Class<?>, List<StatementPermission<?>>> parseConfiguration(InputStream in) throws IOException {
        return parseConfiguration(new InputStreamReader(in));
    }

    private Map<Class<?>, List<StatementPermission<?>>> parseConfiguration(Reader reader) throws IOException {
        Map<Class<?>, List<StatementPermission<?>>> statements = new HashMap<>();
        BufferedReader br = new BufferedReader(reader);
        for(String line = br.readLine(); line != null; line = br.readLine()) {
            for (Map.Entry<Pattern, Function<Matcher, Map.Entry<Class<?>, StatementPermission<?>>>> entry : statementParsers) {
                Pattern pattern = entry.getKey();
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()) {
                    Map.Entry<Class<?>, StatementPermission<?>> statementData = entry.getValue().apply(matcher);
                    statements.merge(statementData.getKey(), List.of(statementData.getValue()), (one, two) -> Stream.concat(one.stream(), two.stream()).collect(toList()));
                }
            }
        }
        return statements;
    }

    private static List<ComparisonPermission> parseComparisonPermissions(String expression) {
        Pattern p = Pattern.compile("([\\w*]+)\\((.+)\\)");
        return Stream.of(expression.split("\\s+")).map(p::matcher).filter(Matcher::find)
                .map(m -> new ComparisonPermission(m.group(1), Stream.of(m.group(2).split(",")).map(ComparisonOperation::valueByOperation).collect(toList())))
                .collect(Collectors.toList());
    }
}

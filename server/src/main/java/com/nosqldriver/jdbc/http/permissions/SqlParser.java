package com.nosqldriver.jdbc.http.permissions;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.StatementVisitorAdapter;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.update.Update;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.EQ;
import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.GE;
import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.GT;
import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.IN;
import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.LE;
import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.LIKE;
import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.LT;
import static com.nosqldriver.jdbc.http.permissions.ComparisonOperation.NE;
import static java.util.Map.entry;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

public class SqlParser {
    private final CCJSqlParserManager parserManager = new CCJSqlParserManager();
    private final Map<Class<? extends Expression>, ComparisonOperation> comparisonExpression = Map.of(
            EqualsTo.class, EQ, NotEqualsTo.class, NE,
            GreaterThanEquals.class, GE, GreaterThan.class, GT,
            MinorThanEquals.class, LE, MinorThan.class, LT,
            InExpression.class, IN,
            LikeExpression.class, LIKE
    );

    public Object parse(String sql) throws JSQLParserException {

        AtomicReference<com.nosqldriver.jdbc.http.permissions.Select> selectRef = new AtomicReference<>();
        AtomicReference<com.nosqldriver.jdbc.http.permissions.Insert> insertRef = new AtomicReference<>();
        AtomicReference<com.nosqldriver.jdbc.http.permissions.Update> updateRef = new AtomicReference<>();
        AtomicReference<com.nosqldriver.jdbc.http.permissions.Delete> deleteRef = new AtomicReference<>();
        AtomicReference<Class<?>> sqlType = new AtomicReference<>();
        Map<Class<?>, AtomicReference<?>> references = Map.of(
                com.nosqldriver.jdbc.http.permissions.Select.class, selectRef,
                com.nosqldriver.jdbc.http.permissions.Insert.class, insertRef,
                com.nosqldriver.jdbc.http.permissions.Update.class, updateRef,
                com.nosqldriver.jdbc.http.permissions.Delete.class, deleteRef);

        parserManager.parse(new StringReader(sql)).accept(new StatementVisitorAdapter(){
            @Override
            public void visit(Insert insert) {
                sqlType.set(com.nosqldriver.jdbc.http.permissions.Insert.class);
                insertRef.set(new com.nosqldriver.jdbc.http.permissions.Insert());
                Table table = insert.getTable();
                insertRef.get().setTable(table.getFullyQualifiedName());
                insertRef.get().setFields(insert.getColumns().stream().map(Column::getColumnName).collect(toList()));
                ofNullable(insert.getItemsList(ExpressionList.class))
                        .map(list -> list.getExpressions().stream().filter(e -> e instanceof RowConstructor).count())
                        .ifPresent(limit -> insertRef.get().setLimit(limit));
            }

            @Override
            public void visit(Update update) {
                sqlType.set(com.nosqldriver.jdbc.http.permissions.Update.class);
                updateRef.set(new com.nosqldriver.jdbc.http.permissions.Update());
                Table table = update.getTable();
                updateRef.get().setTable(table.getFullyQualifiedName());
                updateRef.get().setFields(update.getUpdateSets().stream().flatMap(set -> set.getColumns().stream()).map(Column::getColumnName).collect(toList()));
                updateRef.get().setWhere(getWhereConditions(update.getWhere(), table));
            }

            @Override
            public void visit(Delete delete) {
                sqlType.set(com.nosqldriver.jdbc.http.permissions.Delete.class);
                deleteRef.set(new com.nosqldriver.jdbc.http.permissions.Delete());
                deleteRef.get().setTable(delete.getTable().getFullyQualifiedName());
                deleteRef.get().setWhere(getWhereConditions(delete.getWhere(), delete.getTable()));
            }

            @Override
            public void visit(Select select) {
                sqlType.set(com.nosqldriver.jdbc.http.permissions.Select.class);
                select.getSelectBody().accept(new SelectVisitorAdapter() {
                    @Override
                    public void visit(PlainSelect plainSelect) {
                        selectRef.set(new com.nosqldriver.jdbc.http.permissions.Select());

                        Table table = (Table)plainSelect.getFromItem();
                        selectRef.get().setTable(table.getFullyQualifiedName());
                        selectRef.get().setOuterJoins(getJoins(plainSelect, join -> join.isLeft() || join.isOuter()));
                        selectRef.get().setInnerJoins(getJoins(plainSelect, join -> join.isInner() || !(join.isOuter() || join.isFull() || join.isRight())));
                        selectRef.get().setWhere(getWhereConditions(plainSelect.getWhere(), table));

                        ofNullable(plainSelect.getLimit())
                                .flatMap(limit -> ofNullable(limit.getRowCount()))
                                .ifPresent(count -> count.accept(new ExpressionVisitorAdapter() {
                                    @Override
                                    public void visit(LongValue value) {
                                        selectRef.get().setLimit((int) value.getValue());
                                    }
                                }));

                        List<String> orderByColumns = new ArrayList<>();
                        ofNullable(plainSelect.getOrderByElements())
                                .ifPresent(orderByElements -> orderByElements.forEach(element -> element.getExpression().accept(new ExpressionVisitorAdapter() {
                                    @Override
                                    public void visit(Column column) {
                                        orderByColumns.add(column.getColumnName());
                                    }
                                })));
                        selectRef.get().setOrderBy(orderByColumns);

                        List<String> groupByColumns = ofNullable(plainSelect.getGroupBy()).map(gbe ->
                            gbe.getGroupByExpressionList().getExpressions().stream()
                                    .filter(expr -> expr instanceof Column)
                                    .map(expr -> (Column) expr)
                                    .map(Column::getColumnName).collect(toList())
                        ).orElse(List.of());
                        selectRef.get().setGroupBy(groupByColumns);

                        List<String> fields = new ArrayList<>();
                        plainSelect.getSelectItems().forEach(item -> item.accept(new SelectItemVisitorAdapter() {
                            @Override
                            public void visit(AllColumns columns) {
                                fields.add("*");
                            }

                            @Override
                            public void visit(SelectExpressionItem item) {
                                Expression expression = item.getExpression();
                                if (expression instanceof Column) {
                                    fields.add(((Column)expression).getColumnName());
                                }
                            }
                        }));
                        selectRef.get().setFields(fields);
                    }
                });
            }
        });

        return references.get(sqlType.get()).get();
    }
    
    private List<String> getJoins(PlainSelect plainSelect, Predicate<Join> filter) {
        return ofNullable(plainSelect.getJoins())
                .map(joins -> joins.stream().filter(filter).map(Join::getRightItem).filter(rightItem -> rightItem instanceof Table).map(rightItem -> ((Table)rightItem).getName()).collect(toList()))
                .orElse(List.of());
    }

//    private Entry<String, ComparisonOperation> getCondition(Expression expression) {
//        if (expression instanceof BinaryExpression) {
//            BinaryExpression be = (BinaryExpression)expression;
//            Expression left = be.getLeftExpression();
//            if (left instanceof Column) {
//                return entry(((Column)left).getColumnName(), comparisonExpression.get(expression.getClass()));
//            }
//        }
//
//        return null;
//    }

    private Entry<String, ComparisonOperation> getColumnCondition(Expression e, Table table, Expression subExpression) {
        String tableName = table.getName();
        String tableAlias = ofNullable(table.getAlias()).map(Alias::getName).orElse(null);
        if (subExpression instanceof Column) {
            Column column = (Column) subExpression;
            String columnTableName = ofNullable(column.getTable()).map(Table::getName).orElse(null);
            boolean thisTableColumn = tableAlias != null ? tableAlias.equals(columnTableName) : columnTableName == null || tableName.equals(columnTableName);
            if (thisTableColumn) {
                return entry(column.getColumnName(), comparisonExpression.get(e.getClass()));
            }
        }
        return null;
    }

    private List<Entry<String, ComparisonOperation>> getWhereConditions(Expression whereExpression, Table table) {
        List<Entry<String, ComparisonOperation>> whereConditions = new ArrayList<>();
        ofNullable(whereExpression).ifPresent(where -> where.accept(new ExpressionVisitorAdapter() {
            @Override
            protected void visitBinaryExpression(BinaryExpression be) {
                if (be instanceof OrExpression || be instanceof AndExpression) {
                    visitBinaryExpression((BinaryExpression) be.getLeftExpression());
                    visitBinaryExpression((BinaryExpression) be.getRightExpression());
                } else {
                    ofNullable(getColumnCondition(be, table, be.getLeftExpression())).ifPresent(e -> whereConditions.add(entry(e.getKey(), e.getValue())));
                    ofNullable(getColumnCondition(be, table, be.getRightExpression())).ifPresent(e -> whereConditions.add(entry(e.getKey(), e.getValue())));
                }
            }

            @Override
            public void visit(InExpression in) {
                ofNullable(getColumnCondition(in, table, in.getLeftExpression())).ifPresent(e -> whereConditions.add(entry(e.getKey(), e.getValue())));
            }
        }));

        return whereConditions;
    }
}

package com.clickhouse.parser;

import com.clickhouse.data.FieldInfo;
import com.clickhouse.data.FieldLineageInfo;
import com.clickhouse.data.SelectFieldsInfo;
import com.clickhouse.data.TableInfo;
import com.clickhouse.parser.ast.FromClause;
import com.clickhouse.parser.ast.Identifier;
import com.clickhouse.parser.ast.InsertQuery;
import com.clickhouse.parser.ast.SelectStatement;
import com.clickhouse.parser.ast.TableIdentifier;
import com.clickhouse.parser.ast.expr.AliasColumnExpr;
import com.clickhouse.parser.ast.expr.ColumnExpr;
import com.clickhouse.parser.ast.expr.IdentifierColumnExpr;
import com.clickhouse.parser.ast.expr.TableExpr;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.stream.Collectors;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

/**
 * @author zhouyu
 * @create 2023-02-17 15:30
 */
public class DataLineageDetector extends AstVisitor<Object> {

    /**
     * 插入的表
     */
    private TableInfo toTableInfo;


    /**
     * 插入的字段
     */
    private List<FieldInfo> toColumnList = Lists.newArrayList();

    /**
     * 来源表
     */
    private List<TableInfo> fromTableInfoList = Lists.newArrayList();


    /**
     * 当前select查询id
     */
    private String selectId = null;

    /**
     * 当前select查询父id
     */
    private String selectParentId = null;

    /**
     * 单个字段信息
     */
    private FieldInfo selectFieldInfo = null;

    /**
     * 字段信息临时List
     */
    private List<FieldInfo> fieldInfoTempList;

    /**
     * select查询的id映射关系，key子查询id, value为父查询id
     */
    private Map<String, String> selectParentMap = Maps.newHashMap();

    /**
     * 字段血缘的层次对应关系
     */
    private Map<String, SelectFieldsInfo> selectFieldsInfoMap = Maps.newHashMap();

    /**
     * 是否是别名列
     */
    boolean isAliasColumn = false;

    /**
     * 是否是函数列
     */
    boolean isFunctionColumn = false;

    Stack<String> fromTableAliasStack = new Stack<>();

    Stack<TableInfo> fromTableNameStack = new Stack<>();

    @Override
    public TableInfo visitTableIdentifier(TableIdentifier tableIdentifier) {
        return TableInfo.builder()
                .databaseName(tableIdentifier.getDatabase().getName())
                .tableName(tableIdentifier.getName())
                .build();
    }

    @Override
    public Object visitInsertQuery(InsertQuery insertQuery) {
        if (Objects.nonNull(insertQuery.getTableIdentifier())) {
            this.toTableInfo = visitTableIdentifier(insertQuery.getTableIdentifier());
        }

        if (!CollectionUtils.isEmpty(insertQuery.getColumns())) {
            toColumnList.addAll(
                    insertQuery.getColumns().stream()
                            .map(this::visitIdentifier)
                            .map(fieldName -> FieldInfo.builder()
                                        .fieldName(fieldName)
                                        .tableInfo(toTableInfo)
                                        .build()
                            ).collect(Collectors.toList())
            );
        }

        if (null != insertQuery.getTableFunctionExpr()) {
            visitTableFunctionExpr(insertQuery.getTableFunctionExpr());
        }

        if (null != insertQuery.getDataClause()) {
            visitDataClause(insertQuery.getDataClause());
        }

        return null;
    }

    @Override
    public Object visitSelectStatement(SelectStatement selectStatement) {
        //入栈
        selectParentId = selectId;
        selectId = String.valueOf(selectStatement.hashCode());
        if(StringUtils.isNotEmpty(selectParentId)) {
            selectParentMap.put(selectId, selectParentId);
        }

        SelectFieldsInfo selectFieldsInfo = SelectFieldsInfo.builder()
                .id(selectId)
                .parentId(selectParentId)
                .build();
        selectFieldsInfoMap.put(selectId, selectFieldsInfo);

        Object result = super.visitSelectStatement(selectStatement);

        //出栈
        selectId = selectParentId;
        if(StringUtils.isNotEmpty(selectParentId)) {
            selectParentId = selectParentMap.get(selectParentId);
        }

        return result;
    }


    public Object visitFromClause(FromClause fromClause) {
        SelectFieldsInfo selectFieldsInfo = selectFieldsInfoMap.get(selectId);
        selectFieldsInfo.setSelectFieldsInfo(fieldInfoTempList);
        Object result = super.visitFromClause(fromClause);
        if(!fromTableNameStack.isEmpty()) {
            selectFieldsInfo.setFromTable(fromTableNameStack.pop());
        }
        if(!fromTableAliasStack.isEmpty()) {
            selectFieldsInfo.setTableAlias(fromTableAliasStack.pop());
        }
        return result;
    }

    @Override
    public Object visitColumnExprList(List<ColumnExpr> exprs) {
        if(!(isAliasColumn || isFunctionColumn)) {
            fieldInfoTempList = Lists.newArrayList();
        }
        return super.visitColumnExprList(exprs);
    }


    @Override
    public Object visitColumnExpr(ColumnExpr expr) {
        if(!(isAliasColumn || isFunctionColumn)) {
            selectFieldInfo = FieldInfo.builder()
                    .relatedFieldInfoList(Lists.newArrayList())
                    .build();
            Object result = super.visitColumnExpr(expr);
            fieldInfoTempList.add(selectFieldInfo);
            return result;
        }
        return super.visitColumnExpr(expr);
    }

    @Override
    public Object visitAliasColumnExpr(AliasColumnExpr expr) {
        isAliasColumn = true;
        if(Objects.nonNull(expr.getAlias())) {
            selectFieldInfo.setFieldName(expr.getAlias().getName());
        }
        Object result = super.visitAliasColumnExpr(expr);
        isAliasColumn = false;
        return result;
    }

    @Override
    public Object visitTableExpr(TableExpr tableExpr) {
        //别名
        if (Objects.nonNull(tableExpr.getAlias())) {
            fromTableAliasStack.push(tableExpr.getAlias().getName());
        }
        //表名
        if(Objects.nonNull(tableExpr.getIdentifier())) {
            TableIdentifier tableIdentifier = tableExpr.getIdentifier();
            fromTableNameStack.push(TableInfo.builder()
                    .databaseName(tableIdentifier.getDatabase().getName())
                    .tableName(tableIdentifier.getName())
                    .build());
        }

        return super.visitTableExpr(tableExpr);
    }


    @Override
    public Object visitIdentifierColumnExpr(ColumnExpr expr) {
        if(Objects.isNull(expr) || !(expr instanceof  IdentifierColumnExpr)) {
            return super.visitIdentifierColumnExpr(expr);
        }

        IdentifierColumnExpr identifierColumnExpr = (IdentifierColumnExpr) expr;
        TableIdentifier tableIdentifier = identifierColumnExpr.getIdentifier().getTable();

        TableInfo tableInfo = null;
        if(Objects.nonNull(tableIdentifier)) {
            tableInfo = TableInfo.builder().build();
            tableInfo.setTableName(tableIdentifier.getName());
            if(Objects.nonNull(tableIdentifier.getDatabase())) {
                tableInfo.setDatabaseName(tableIdentifier.getDatabase().getName());
            }
        }

        if(isAliasColumn || isFunctionColumn) {
            selectFieldInfo.getRelatedFieldInfoList().add(FieldInfo.builder()
                            .tableInfo(tableInfo)
                            .fieldName(identifierColumnExpr.getIdentifier().getName())
                    .build());
        } else {
            selectFieldInfo.setTableInfo(tableInfo);
            selectFieldInfo.setFieldName(identifierColumnExpr.getIdentifier().getName());
        }

        return super.visitIdentifierColumnExpr(expr);
    }

    @Override
    public Object visitLiteralColumnExpr(ColumnExpr expr) {
        return super.visitLiteralColumnExpr(expr);
    }

    public Object visitFunctionColumnExpr(ColumnExpr expr) {
        isFunctionColumn = true;
        Object result = super.visitFunctionColumnExpr(expr);
        isFunctionColumn = false;
        return result;
    }

    @Override
    public Object visitSubqueryColumnExpr(ColumnExpr expr) {
        return super.visitSubqueryColumnExpr(expr);
    }

    @Override
    public String visitIdentifier(Identifier identifier) {
        return identifier.getName();
    }

    private List<FieldInfo> sourceFieldInfoList;

    public List<FieldLineageInfo> getFieldLineage() {
        List<FieldInfo> targetFieldInfoList = getTargetFields();
        return targetFieldInfoList.stream()
                .map(fieldInfo -> {
                    sourceFieldInfoList = Lists.newArrayList();
                    return FieldLineageInfo.builder()
                            .targetField(fieldInfo)
                            .sourceFields(sourceFieldInfoList)
                            .build();
                }).collect(Collectors.toList());

    }


    public void getSourceFieldInfo(String targetField, String parentId) {
        for(SelectFieldsInfo selectFieldsInfo : selectFieldsInfoMap.values()) {
            if(StringUtils.isNotEmpty(selectFieldsInfo.getParentId()) && StringUtils.equals(selectFieldsInfo.getParentId(), parentId)) {
                continue;
            }

            if(StringUtils.isEmpty(selectFieldsInfo.getParentId()) && StringUtils.isNotEmpty(selectFieldsInfo.getParentId())) {
                continue;
            }

            if(CollectionUtils.isEmpty(selectFieldsInfo.getSelectFieldsInfo())) {
                continue;
            }

            if(Objects.isNull(selectFieldsInfo.getFromTable())) {
                selectFieldsInfo.getSelectFieldsInfo().stream()
                        .forEach(fieldInfo -> {

                        });
            }

        }
    }



    /**
     * 获取目标字段，即insert列或者最外层select
     * @return
     */
    public List<FieldInfo> getTargetFields() {
        return selectFieldsInfoMap.values().stream()
                .filter(selectFieldsInfo -> StringUtils.isEmpty(selectFieldsInfo.getParentId()))
                .map(SelectFieldsInfo::getSelectFieldsInfo)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }



}

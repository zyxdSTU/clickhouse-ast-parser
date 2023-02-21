package com.clickhouse.parser;

import com.clickhouse.data.FieldInfo;
import com.clickhouse.data.FieldLineageInfo;
import com.clickhouse.data.TableInfo;
import com.clickhouse.parser.ast.FromClause;
import com.clickhouse.parser.ast.Identifier;
import com.clickhouse.parser.ast.InsertQuery;
import com.clickhouse.parser.ast.SelectStatement;
import com.clickhouse.parser.ast.SelectUnionQuery;
import com.clickhouse.parser.ast.TableIdentifier;
import com.clickhouse.parser.ast.expr.AliasColumnExpr;
import com.clickhouse.parser.ast.expr.AsteriskColumnExpr;
import com.clickhouse.parser.ast.expr.ColumnExpr;
import com.clickhouse.parser.ast.expr.FunctionColumnExpr;
import com.clickhouse.parser.ast.expr.IdentifierColumnExpr;
import com.clickhouse.parser.ast.expr.LiteralColumnExpr;
import com.clickhouse.parser.ast.expr.SubqueryColumnExpr;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
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
    private List<String> toColumnList = Lists.newArrayList();

    /**
     * 来源表
     */
    private List<TableInfo> fromTableInfoList = Lists.newArrayList();

    /**
     * 来源字段
     */
    private List<String> fromColumnList = Lists.newArrayList();


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
    private FieldInfo fieldInfo = null;

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
    private Map<String, FieldLineageInfo> fieldLineageInfoMap = Maps.newHashMap();

    /**
     * 是否是别名列
     */
    boolean isAliasColumn = false;

    /**
     * 是否是函数列
     */
    boolean isFunctionColumn = false;

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
                            .collect(Collectors.toList())
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

        FieldLineageInfo fieldLineageInfo = FieldLineageInfo.builder()
                .id(selectId)
                .parentId(selectParentId)
                .build();
        fieldLineageInfoMap.put(selectId, fieldLineageInfo);

        Object result = super.visitSelectStatement(selectStatement);

        //出栈
        selectId = selectParentId;
        if(StringUtils.isNotEmpty(selectParentId)) {
            selectParentId = selectParentMap.get(selectParentId);
        }

        return result;
    }


    public Object visitFromClause(FromClause fromClause) {
        fieldLineageInfoMap.get(selectId).setSelectFieldInfoList(fieldInfoTempList);
        return super.visitFromClause(fromClause);
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
            fieldInfo = FieldInfo.builder()
                    .relatedFieldName(Lists.newArrayList())
                    .build();
            Object result = super.visitColumnExpr(expr);
            fieldInfoTempList.add(fieldInfo);
            return result;
        }
        return super.visitColumnExpr(expr);
    }

    @Override
    public Object visitAliasColumnExpr(AliasColumnExpr expr) {
        isAliasColumn = true;
        if(Objects.nonNull(expr.getAlias())) {
            fieldInfo.setFieldName(expr.getAlias().getName());
        }
        Object result = super.visitAliasColumnExpr(expr);
        isAliasColumn = false;
        return result;
    }


    @Override
    public Object visitIdentifierColumnExpr(ColumnExpr expr) {
        if (Objects.nonNull(expr) && expr instanceof IdentifierColumnExpr) {
            IdentifierColumnExpr identifierColumnExpr = (IdentifierColumnExpr) expr;
            if(isAliasColumn || isFunctionColumn) {
                fieldInfo.getRelatedFieldName().add((identifierColumnExpr.getIdentifier().getName()));
            } else {
                fieldInfo.setFieldName(identifierColumnExpr.getIdentifier().getName());
            }
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
}

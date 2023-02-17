package com.clickhouse.parser;

import com.clickhouse.parser.ast.Identifier;
import com.clickhouse.parser.ast.InsertQuery;
import com.clickhouse.parser.ast.SelectStatement;
import com.clickhouse.parser.ast.TableIdentifier;
import com.clickhouse.parser.ast.expr.ColumnExpr;
import com.clickhouse.parser.ast.expr.IdentifierColumnExpr;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.collections.CollectionUtils;

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

        if (CollectionUtils.isEmpty(insertQuery.getColumns())) {
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

    /**
     * 目前只支持命名字段
     *
     * @param expr
     * @return
     */
    @Override
    public String visitColumnExpr(ColumnExpr expr) {
        if (expr instanceof IdentifierColumnExpr) {
            return visitIdentifierColumnExpr(expr);
        }
        return null;
    }

    @Override
    public Object visitSelectStatement(SelectStatement selectStatement) {
        //select列
        if (CollectionUtils.isNotEmpty(selectStatement.getExprs())) {
            fromColumnList.addAll(
                    selectStatement.getExprs().stream()
                            .map(this::visitColumnExpr)
                            .collect(Collectors.toList())
            );
        }

        //select表
        if (null != selectStatement.getFromClause()) {
            visitFromClause(selectStatement.getFromClause());
        }

        return null;
    }


    @Override
    public String visitIdentifierColumnExpr(ColumnExpr expr) {
        if (Objects.nonNull(expr) && expr instanceof IdentifierColumnExpr) {
            IdentifierColumnExpr identifierColumnExpr = (IdentifierColumnExpr) expr;
            return visitIdentifier(identifierColumnExpr.getIdentifier());
        }
        return null;
    }

    @Override
    public String visitIdentifier(Identifier identifier) {
        return identifier.getName();
    }
}

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
class TableInfo {

    String tableName;
    String databaseName;
}

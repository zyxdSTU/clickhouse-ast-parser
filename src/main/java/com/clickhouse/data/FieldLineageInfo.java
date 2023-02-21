package com.clickhouse.data;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhouyu
 * @create 2023-02-21 11:08
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FieldLineageInfo {

    /**
     * id
     */
    String id;

    /**
     * 父id
     */
    String parentId;

    /**
     * 来源表，来源为子查询，则为空
     */
    TableInfo fromTable;


    /**
     * 表别名
     */
    String tableAlias;

    /**
     * select字段
     */
    List<FieldInfo> selectFieldInfoList;

}

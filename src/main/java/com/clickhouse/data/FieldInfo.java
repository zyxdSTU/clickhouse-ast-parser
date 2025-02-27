package com.clickhouse.data;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhouyu
 * @create 2023-02-21 11:01
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FieldInfo {

    /**
     * 与其关联的字段名
     */
    List<FieldInfo> relatedFieldInfoList;

    /**
     * 字段名称
     */
    String fieldName;


    /**
     * 表信息
     */
    TableInfo tableInfo;

    /**
     * 函数
     */
    String process;
}

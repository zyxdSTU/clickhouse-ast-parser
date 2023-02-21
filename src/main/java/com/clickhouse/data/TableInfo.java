package com.clickhouse.data;

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
public class TableInfo {

    String tableName;
    String databaseName;
}
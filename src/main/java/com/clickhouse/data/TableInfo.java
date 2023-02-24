package com.clickhouse.data;

import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang.StringUtils;

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

    public boolean isBlank() {
        return StringUtils.isEmpty(tableName);
    }

    public static boolean isNull(TableInfo tableInfo) {
        if(Objects.isNull(tableInfo)) {
            return true;
        }

        if(StringUtils.isEmpty(tableInfo.getTableName())) {
            return true;
        }

        return false;
    }
}
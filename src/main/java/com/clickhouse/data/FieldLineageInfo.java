package com.clickhouse.data;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhouyu
 * @create 2023-02-22 14:02
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class FieldLineageInfo {
    private FieldInfo targetField;
    private List<FieldInfo> sourceFields;
}

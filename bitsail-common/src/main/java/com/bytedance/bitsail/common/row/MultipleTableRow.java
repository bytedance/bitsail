/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.common.row;

import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.stream.Collectors;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Builder
public class MultipleTableRow implements Serializable {

  private static final RowTypeInfo MULTIPLE_TABLE_ROW_TYPE_INFO =
      new RowTypeInfo(Arrays.stream(MultipleTableField.values())
          .map(MultipleTableField::getName)
          .collect(Collectors.toList())
          .toArray(new String[] {}),

          Arrays.stream(MultipleTableField.values())
              .map(MultipleTableField::getTypeInfo)
              .collect(Collectors.toList())
              .toArray(new TypeInfo[] {}));

  private String tableId;

  private String key;

  private String value;

  private String offset;

  private String partition;

  public static MultipleTableRow of(String tableId,
                                    String key,
                                    String value,
                                    String offset,
                                    String partition) {
    return MultipleTableRow
        .builder()
        .tableId(tableId)
        .key(key)
        .value(value)
        .offset(offset)
        .partition(partition)
        .build();
  }

  public static MultipleTableRow of(Row row) {
    return MultipleTableRow
        .builder()
        .tableId(row.getString(MultipleTableField.TABLE_ID_FIELD.index))
        .key(row.getString(MultipleTableField.KEY_FIELD.index))
        .value(row.getString(MultipleTableField.VALUE_FIELD.index))
        .offset(row.getString(MultipleTableField.OFFSET_FIELD.index))
        .partition(row.getString(MultipleTableField.PARTITION_FIELD.index))
        .build();
  }

  public Row asRow() {
    Row row = new Row(MultipleTableField.values().length);
    row.setField(MultipleTableField.TABLE_ID_FIELD.index, tableId);
    row.setField(MultipleTableField.KEY_FIELD.index, key);
    row.setField(MultipleTableField.VALUE_FIELD.index, value);
    row.setField(MultipleTableField.OFFSET_FIELD.index, offset);
    row.setField(MultipleTableField.PARTITION_FIELD.index, partition);
    return row;
  }

  public RowTypeInfo getRowTypeInfo() {
    return MULTIPLE_TABLE_ROW_TYPE_INFO;
  }

  @Getter
  private enum MultipleTableField {
    TABLE_ID_FIELD("table-id", TypeInfos.STRING_TYPE_INFO, 0),
    KEY_FIELD("key", TypeInfos.STRING_TYPE_INFO, 1),
    VALUE_FIELD("value", TypeInfos.STRING_TYPE_INFO, 2),
    OFFSET_FIELD("offset", TypeInfos.STRING_TYPE_INFO, 3),
    PARTITION_FIELD("partition", TypeInfos.STRING_TYPE_INFO, 4);

    private final String name;

    private final TypeInfo<?> typeInfo;

    private final int index;

    MultipleTableField(String name, TypeInfo<?> typeInfo, int index) {
      this.name = name;
      this.typeInfo = typeInfo;
      this.index = index;
    }
  }
}

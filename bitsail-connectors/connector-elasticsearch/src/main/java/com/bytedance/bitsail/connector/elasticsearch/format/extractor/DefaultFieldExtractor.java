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

package com.bytedance.bitsail.connector.elasticsearch.format.extractor;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoValueConverter;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;
import com.bytedance.bitsail.connector.elasticsearch.exception.ElasticsearchErrorCode;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class DefaultFieldExtractor implements Function<Row, String>, Serializable {

  private final String delimiter;

  private final List<Integer> fieldIndexes;

  private final RowTypeInfo rowTypeInfo;

  private final TypeInfoValueConverter valueConverter;

  private DefaultFieldExtractor(TypeInfoValueConverter valueConverter,
                                RowTypeInfo rowTypeInfo,
                                String delimiter,
                                List<String> fields) {
    this.delimiter = delimiter;
    this.rowTypeInfo = rowTypeInfo;
    this.valueConverter = valueConverter;
    this.fieldIndexes = new ArrayList<>(fields.size());
    for (String field : fields) {
      int i = rowTypeInfo.indexOf(field);
      if (i == -1) {
        throw BitSailException.asBitSailException(ElasticsearchErrorCode.ELASTICSEARCH_FIELD_NOT_EXIST_ERROR,
            String.format("Elasticsearch configure field %s not exists in row type info.", field));
      }
      fieldIndexes.add(i);
    }
  }

  public static Function<Row, String> createFieldsExtractor(TypeInfoValueConverter valueConverter,
                                                            RowTypeInfo rowTypeInfo,
                                                            String delimiter,
                                                            List<String> fields) {
    if (CollectionUtils.isEmpty(fields)) {
      return row -> null;
    }
    return new DefaultFieldExtractor(valueConverter,
        rowTypeInfo,
        delimiter,
        fields);
  }

  public static Function<Row, String> createFieldExtractor(TypeInfoValueConverter valueConverter,
                                                           RowTypeInfo rowTypeInfo,
                                                           String field) {
    if (StringUtils.isEmpty(field)) {
      return row -> null;
    }
    return new DefaultFieldExtractor(valueConverter,
        rowTypeInfo,
        StringUtils.EMPTY,
        Lists.newArrayList(field));
  }

  @Override
  public String apply(Row row) {
    List<String> values = Lists.newArrayListWithCapacity(fieldIndexes.size());
    for (int index = 0; index < fieldIndexes.size(); index++) {
      Integer fieldIndex = fieldIndexes.get(index);
      Object value = row.getField(fieldIndex);
      values.add((String) valueConverter.convertObject(value, TypeInfos.STRING_TYPE_INFO));
    }

    return StringUtils.join(values, delimiter);
  }
}

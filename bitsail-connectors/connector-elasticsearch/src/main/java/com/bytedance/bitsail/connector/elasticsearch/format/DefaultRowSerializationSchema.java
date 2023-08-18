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

package com.bytedance.bitsail.connector.elasticsearch.format;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.row.RowKind;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoValueConverter;
import com.bytedance.bitsail.connector.elasticsearch.constants.Elasticsearchs;
import com.bytedance.bitsail.connector.elasticsearch.format.extractor.DefaultFieldExtractor;
import com.bytedance.bitsail.connector.elasticsearch.format.extractor.DefaultValueExtractor;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchOptions;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.function.Function;

public class DefaultRowSerializationSchema implements ElasticsearchRowSerializationSchema {

  private final Function<Row, String> indexNameFunction;
  private final Function<Row, String> primaryKeyFunction;
  private final Function<Row, String> routingKeyFunction;
  private final Function<Row, String> documentFunction;

  public DefaultRowSerializationSchema(BitSailConfiguration commonConfiguration,
                                       BitSailConfiguration writerConfiguration,
                                       RowTypeInfo rowTypeInfo) {
    TypeInfoValueConverter valueConverter = new TypeInfoValueConverter(commonConfiguration);
    this.indexNameFunction = getIndexNameFunction(valueConverter, writerConfiguration, rowTypeInfo);
    this.primaryKeyFunction = DefaultFieldExtractor
        .createFieldsExtractor(valueConverter, rowTypeInfo,
            writerConfiguration.get(ElasticsearchOptions.ID_FIELD_DELIMITER),
            Lists.newArrayList(StringUtils.split(writerConfiguration
                .get(ElasticsearchOptions.ID_KEY_FIELDS), Elasticsearchs.COMMA)
            )
        );
    this.routingKeyFunction = DefaultFieldExtractor.createFieldExtractor(valueConverter, rowTypeInfo,
        writerConfiguration.get(ElasticsearchOptions.ROUTING_KEY_FIELD));
    this.documentFunction = new DefaultValueExtractor(valueConverter, rowTypeInfo);
  }

  protected static Function<Row, String> getIndexNameFunction(TypeInfoValueConverter valueConverter,
                                                              BitSailConfiguration writerConfiguration,
                                                              RowTypeInfo rowTypeInfo) {
    String indexName = writerConfiguration.get(ElasticsearchOptions.INDEX);
    if (StringUtils.isNotEmpty(indexName)) {
      return (row -> indexName);
    }

    String indexKeyField = writerConfiguration.get(ElasticsearchOptions.INDEX_KEY_FIELD);
    if (StringUtils.isNotEmpty(indexKeyField)) {
      return DefaultFieldExtractor.createFieldExtractor(
          valueConverter,
          rowTypeInfo,
          indexKeyField);
    }

    throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
        String.format("Index name can't be confirm from configuration, please check the config name %s or %s.",
            ElasticsearchOptions.INDEX.key(), ElasticsearchOptions.INDEX_KEY_FIELD.key()));
  }

  @Override
  public DocWriteRequest<?> serialize(Row row) {
    String indexName = indexNameFunction.apply(row);
    String id = primaryKeyFunction.apply(row);
    String routing = routingKeyFunction.apply(row);
    String document = null;
    RowKind kind = row.getKind();
    switch (kind) {
      case INSERT:
        //id could be null
        document = documentFunction.apply(row);
        return new IndexRequest()
            .index(indexName)
            .id(id)
            .routing(routing)
            .source(document, XContentType.JSON);
      case UPDATE_AFTER:
        //id can't be null
        document = documentFunction.apply(row);
        return new UpdateRequest()
            .index(indexName)
            .id(id)
            .routing(routing)
            .doc(document, XContentType.JSON)
            .docAsUpsert(true);

      case UPDATE_BEFORE:
      case DELETE:
        //id can't be null
        return new DeleteRequest()
            .index(indexName)
            .id(id)
            .routing(routing);
      default:
        throw BitSailException.asBitSailException(
            CommonErrorCode.INTERNAL_ERROR,
            "Unsupported row kind: " + kind);
    }
  }
}

/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.elasticsearch.doc;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.elasticsearch.doc.parameter.EsDocParameters;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchWriterOptions;
import com.bytedance.bitsail.connector.elasticsearch.rest.EsRequestEmitter;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

public class EsRequestConstructorTest {

  private final Object[] fields = new Object[] {
      100,
      "varchar",
      "text",
      "bigint",
      "20220810",
      "es_index_20220810",
      "upsert",
      10
  };
  private final Row row = new Row(fields);

  @Test
  public void testCreateRequest() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(new File(
        Paths.get(getClass().getClassLoader().getResource("es_doc_parameter_test.json").toURI()).toString()));
    jobConf.set(ElasticsearchWriterOptions.DOC_EXCLUDE_FIELDS,
        "op_type,version");

    EsDocParameters parameters = EsRequestEmitter.initEsDocParams(jobConf);
    EsRequestConstructor constructor = new EsRequestConstructor(jobConf, parameters);
    ActionRequest actionRequest = constructor.createRequest(row);
    Assert.assertTrue(actionRequest instanceof UpdateRequest);
    UpdateRequest request = (UpdateRequest) actionRequest;
    Assert.assertEquals("es_index_20220810", request.index());
    Assert.assertEquals("_doc", request.type());
  }
}

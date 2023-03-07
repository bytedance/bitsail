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
import com.bytedance.bitsail.connector.elasticsearch.doc.parameter.EsDocParameters;
import com.bytedance.bitsail.connector.elasticsearch.rest.EsRequestEmitter;

import com.alibaba.fastjson.serializer.SerializerFeature;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;

public class EsDocParameterTest {

  @Test
  public void testInitializeEsDocParams() throws Exception {
    BitSailConfiguration jobConf = BitSailConfiguration.from(new File(
        Paths.get(getClass().getClassLoader().getResource("es_doc_parameter_test.json").toURI()).toString()));
    EsDocParameters params = EsRequestEmitter.initEsDocParams(jobConf);

    Assert.assertEquals(0, params.getIdFieldsIndices().get(0).intValue());
    Assert.assertEquals(4, params.getIdFieldsIndices().get(1).intValue());
    Assert.assertEquals(1, params.getExcludedFieldsIndices().get(0).intValue());
    Assert.assertEquals(0, params.getRoutingFieldsIndices().get(0).intValue());

    Assert.assertEquals(",", params.getIdDelimiter());
    Assert.assertEquals(5, params.getDynamicFieldIndex().intValue());
    Assert.assertEquals(6, params.getOpTypeIndex().intValue());
    Assert.assertEquals(7, params.getVersionIndex().intValue());

    Assert.assertTrue(params.isIgnoreBlankValue());
    Assert.assertFalse(params.isFlattenMap());
    Assert.assertEquals(SerializerFeature.QuoteFieldNames, params.getJsonFeatures().get(0));
    Assert.assertEquals(SerializerFeature.UseSingleQuotes, params.getJsonFeatures().get(1));
  }
}

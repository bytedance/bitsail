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

package com.bytedance.bitsail.connector.legacy.hudi.dag;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import org.junit.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HudiSinkFunctionDAGBuilderTest {
  @Test
  public void testExtractConfig() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set("job.writer.hoodie.recordkey.field", "id1,id2");
    Map<String, String> hudiConf = HudiSinkFunctionDAGBuilder.extractHudiProperties(jobConf);
    assertEquals("id1,id2", hudiConf.get("hoodie.recordkey.field"));
  }
}

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

package com.bytedance.bitsail.component.format.debezium;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class DebeziumRowDeserializationSchemaTest {

  @Test
  public void test() throws URISyntaxException, IOException {
    byte[] bytes = Files.readAllBytes(Paths.get(DebeziumRowDeserializationSchemaTest
        .class.getClassLoader().getResource("file/debezium.json")
        .toURI().getPath()));
    DebeziumRowDeserializationSchema debeziumRowDeserializationSchema =
        new DebeziumRowDeserializationSchema(BitSailConfiguration.newDefault());

    Row deserialize = debeziumRowDeserializationSchema.deserialize(new String(bytes),
        new String[] {"double_type"});

    Assert.assertNotNull(deserialize);
    Assert.assertTrue(deserialize.getField(0) instanceof BigDecimal);
  }

}
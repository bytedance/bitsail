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
import com.bytedance.bitsail.common.row.RowKind;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

public class DebeziumRowDeserializationSchemaTest {

  private DebeziumRowDeserializationSchema deserializationSchema;

  @Before
  public void before() {
    deserializationSchema = new DebeziumRowDeserializationSchema(BitSailConfiguration.newDefault());
  }

  @Test
  public void test() throws URISyntaxException, IOException {
    byte[] bytes = Files.readAllBytes(Paths.get(DebeziumRowDeserializationSchemaTest
        .class.getClassLoader().getResource("file/debezium.json")
        .toURI().getPath()));

    Row deserialize = deserializationSchema.deserialize(new String(bytes),
        new String[] {"double_type"});

    Assert.assertNotNull(deserialize);
    Assert.assertTrue(deserialize.getField(0) instanceof BigDecimal);
  }

  @Test
  public void testInsert() throws URISyntaxException, IOException {
    byte[] bytes = Files.readAllBytes(Paths.get(DebeziumRowDeserializationSchemaTest
        .class.getClassLoader().getResource("file/debezium_insert.json")
        .toURI().getPath()));

    Row deserialize = deserializationSchema.deserialize(new String(bytes),
        new String[] {"double_type"});

    Assert.assertNotNull(deserialize);
    Assert.assertEquals(deserialize.getKind(), RowKind.INSERT);
  }

  @Test
  public void testUpsert() throws URISyntaxException, IOException {
    byte[] bytes = Files.readAllBytes(Paths.get(DebeziumRowDeserializationSchemaTest
        .class.getClassLoader().getResource("file/debezium_upsert.json")
        .toURI().getPath()));

    Row deserialize = deserializationSchema.deserialize(new String(bytes),
        new String[] {"order_date"});

    Assert.assertNotNull(deserialize);
    Assert.assertEquals(LocalDate.ofEpochDay(16816).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(),
        ((Date) deserialize.getField(0)).toInstant().toEpochMilli());
    Assert.assertEquals(deserialize.getKind(), RowKind.UPDATE_AFTER);
  }

  @Test
  public void testDelete() throws URISyntaxException, IOException {
    byte[] bytes = Files.readAllBytes(Paths.get(DebeziumRowDeserializationSchemaTest
        .class.getClassLoader().getResource("file/postgres/debezium_pg_delete.json")
        .toURI().getPath()));

    Row deserialize = deserializationSchema.deserialize(new String(bytes),
        new String[] {"id"});

    Assert.assertNotNull(deserialize);
    Assert.assertEquals(deserialize.getField(0), 10049L);
    Assert.assertEquals(deserialize.getKind(), RowKind.DELETE);
  }

}
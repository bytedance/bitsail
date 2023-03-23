/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.mysql.source.sql;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class MysqlTableDescriberTest {
  @Test
  public void testTableDes() {
    List<MysqlFieldDescriber> fields = new ArrayList<>();
    List<String> primaryKeys = new ArrayList<>();
    MysqlFieldDescriber pk = new MysqlFieldDescriber();
    pk.setColumnName("key_field");
    pk.setColumnType("INT");
    pk.setNullable(false);
    pk.setKey(true);
    pk.setUnique(false);
    pk.setDefaultValue("NULL");
    pk.setExtra("Extra props");

    MysqlFieldDescriber field1 = new MysqlFieldDescriber();
    field1.setColumnName("first_field");
    field1.setColumnType("VARCHAR(1024) CHARSET utf8mb4");
    field1.setNullable(true);
    field1.setKey(false);
    field1.setUnique(false);
    field1.setDefaultValue("");
    field1.setExtra("");

    fields.add(pk);
    fields.add(field1);
    primaryKeys.add("key_field");
    String ddl = new MysqlTableDescriber("`db.table`", fields, primaryKeys).toDdl();
    Assert.assertEquals("CREATE TABLE `db.table` (\n" +
        "\t `key_field` INT NOT NULL, \n" +
        "\t`first_field` VARCHAR(1024) CHARSET utf8mb4  ,PRIMARY KEY ( `key_field` ) );", ddl);
  }
}

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

package com.bytedance.bitsail.connector.cdc.mysql.source.debezium;

import io.debezium.document.Array;
import io.debezium.document.DocumentReader;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.Collect;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Types;
import java.util.Map;

public class InMemoryDatabaseHistoryTest {

  @Test
  public void canSerializeAndDeserializeHistoryRecord() throws Exception {
    Map<String, Object> source = Collect.linkMapOf("server", "abc");
    Map<String, Object> position = Collect.linkMapOf("file", "x.log", "positionInt", 100, "positionLong", Long.MAX_VALUE, "entry", 1);
    String databaseName = "db";
    String schemaName = "myschema";
    String ddl = "CREATE TABLE foo ( first VARCHAR(22) NOT NULL );";

    Table table = Table.editor()
        .tableId(new TableId(databaseName, schemaName, "foo"))
        .addColumn(Column.editor()
            .name("first")
            .jdbcType(Types.VARCHAR)
            .type("VARCHAR")
            .length(22)
            .optional(false)
            .create())
        .setPrimaryKeyNames("first")
        .create();

    TableChanges tableChanges = new TableChanges().create(table);

    HistoryRecord record = new HistoryRecord(source, position, databaseName, schemaName, ddl, tableChanges);

    String serialized = record.toString();
    DocumentReader reader = DocumentReader.defaultReader();
    HistoryRecord deserialized = new HistoryRecord(reader.read(serialized));

    Assert.assertNotNull(deserialized.document().getDocument(HistoryRecord.Fields.SOURCE));
    Assert.assertEquals("abc", deserialized.document().getDocument(HistoryRecord.Fields.SOURCE).getString("server"));

    Assert.assertNotNull(deserialized.document().getDocument(HistoryRecord.Fields.POSITION));
    Assert.assertEquals("x.log", deserialized.document().getDocument(HistoryRecord.Fields.POSITION).getString("file"));
    Assert.assertEquals(100, deserialized.document().getDocument(HistoryRecord.Fields.POSITION).getInteger("positionInt").intValue());

    Assert.assertEquals(Long.MAX_VALUE, deserialized.document().getDocument(HistoryRecord.Fields.POSITION).getLong("positionLong").longValue());
    Assert.assertEquals(1, deserialized.document().getDocument(HistoryRecord.Fields.POSITION).getInteger("entry").intValue());
    Assert.assertEquals(databaseName, deserialized.document().getString(HistoryRecord.Fields.DATABASE_NAME));
    Assert.assertEquals(schemaName, deserialized.document().getString(HistoryRecord.Fields.SCHEMA_NAME));

    Assert.assertEquals(ddl, deserialized.document().getString(HistoryRecord.Fields.DDL_STATEMENTS));

    final TableChanges.TableChangesSerializer<Array> tableChangesSerializer = new JsonTableChangeSerializer();
    Assert.assertEquals(tableChanges, tableChangesSerializer.deserialize(deserialized.document().getArray(HistoryRecord.Fields.TABLE_CHANGES), true));
  }
}

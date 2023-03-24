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

package com.bytedance.bitsail.connector.cdc.mysql.source.schema;

import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Converter between debezium schema recognized by DatabaseHistory and serialized String schema in split.
 */
public class TableChangeConverter {

  public static Map<TableId, TableChanges.TableChange> stringToTableChange(Map<String, String> schemas) throws IOException {
    Map<TableId, TableChanges.TableChange> results = new HashMap<>();

    DocumentReader reader = DocumentReader.defaultReader();
    for (String serialized : schemas.values()) {
      Document document = reader.read(serialized);
      TableChanges.TableChange tableChange = JsonTableChangeSerializer.fromDocument(document, true);
      results.put(tableChange.getId(), tableChange);
    }
    return results;
  }

  public static Map<String, String> tableChangeToString(Map<TableId, TableChanges.TableChange> tableChanges) throws IOException {
    Map<String, String> result = new HashMap<>();
    DocumentWriter documentWriter = DocumentWriter.defaultWriter();
    JsonTableChangeSerializer jsonTableChangeSerializer = new JsonTableChangeSerializer();
    for (TableChanges.TableChange tableChange : tableChanges.values()) {
      final String key = tableChange.getTable().id().toString();
      final String val = documentWriter.write(jsonTableChangeSerializer.toDocument(tableChange));
      result.put(key, val);
    }
    return result;
  }
}

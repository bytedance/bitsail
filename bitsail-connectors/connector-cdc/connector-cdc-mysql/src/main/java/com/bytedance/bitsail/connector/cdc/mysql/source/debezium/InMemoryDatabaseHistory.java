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

import io.debezium.config.Configuration;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.DatabaseHistory;
import io.debezium.relational.history.DatabaseHistoryException;
import io.debezium.relational.history.DatabaseHistoryListener;
import io.debezium.relational.history.HistoryRecord;
import io.debezium.relational.history.HistoryRecordComparator;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Debezium DatabaseHistory instance that store in the memory of the reader task.
 */
public class InMemoryDatabaseHistory implements DatabaseHistory {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDatabaseHistory.class);

  public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

  public static final ConcurrentMap<String, Collection<TableChanges.TableChange>> TABLE_SCHEMAS =
      new ConcurrentHashMap<>();
  private Map<TableId, TableChanges.TableChange> tableSchemas;
  private DatabaseHistoryListener listener;
  private boolean storeOnlyMonitoredTablesDdl;
  private boolean skipUnparseableDDL;

  @Override
  public void configure(Configuration config, HistoryRecordComparator comparator, DatabaseHistoryListener listener, boolean useCatalogBeforeSchema) {
    this.listener = listener;
    this.storeOnlyMonitoredTablesDdl = config.getBoolean(STORE_ONLY_MONITORED_TABLES_DDL);
    this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);
    String instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
    this.tableSchemas = new HashMap<>();
    for (TableChanges.TableChange tableChange : removeHistory(instanceName)) {
      tableSchemas.put(tableChange.getId(), tableChange);
    }
  }

  @Override
  public void start() {
    listener.started();
  }

  @Override
  public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl) throws DatabaseHistoryException {
    throw new UnsupportedOperationException("should not call here, error");
  }

  @Override
  public void record(Map<String, ?> source, Map<String, ?> position, String databaseName, String schemaName, String ddl, TableChanges changes)
      throws DatabaseHistoryException {
    final HistoryRecord record =
        new HistoryRecord(source, position, databaseName, schemaName, ddl, changes);
    LOG.debug("Received DDL record {}", record);
    listener.onChangeApplied(record);
  }

  @Override
  public void recover(Map<String, ?> source, Map<String, ?> position, Tables schema, DdlParser ddlParser) {
    LOG.debug("Recovering database history");
    listener.recoveryStarted();
    for (TableChanges.TableChange tableChange : tableSchemas.values()) {
      schema.overwriteTable(tableChange.getTable());
    }
    listener.recoveryStopped();
  }

  @Override
  public void stop() {
    listener.stopped();
  }

  @Override
  public boolean exists() {
    return tableSchemas != null && !tableSchemas.isEmpty();
  }

  @Override
  public boolean storageExists() {
    return true;
  }

  @Override
  public void initializeStorage() {

  }

  @Override
  public boolean storeOnlyCapturedTables() {
    return storeOnlyMonitoredTablesDdl;
  }

  @Override
  public boolean skipUnparseableDdlStatements() {
    return skipUnparseableDDL;
  }

  public static void registerHistory(String engineName, Collection<TableChanges.TableChange> engineHistory) {
    TABLE_SCHEMAS.put(engineName, engineHistory);
  }

  public static Collection<TableChanges.TableChange> removeHistory(String engineName) {
    if (engineName == null) {
      return Collections.emptyList();
    }
    Collection<TableChanges.TableChange> tableChanges = TABLE_SCHEMAS.remove(engineName);
    return tableChanges != null ? tableChanges : Collections.emptyList();
  }
}

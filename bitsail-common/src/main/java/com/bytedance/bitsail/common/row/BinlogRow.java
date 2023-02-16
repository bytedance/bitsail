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

package com.bytedance.bitsail.common.row;

/**
 * Intermedia data structure of binlog row.
 */
@SuppressWarnings("checkstyle:MagicNumber")
public class BinlogRow extends Row {
  private static final int ROW_SIZE = 7;
  private static final int DATABASE_INDEX = 0;
  private static final int TABLE_INDEX = 1;
  private static final int KEY_INDEX = 2;
  private static final int TIMESTAMP_INDEX = 3;
  private static final int DDL_FLAG_INDEX = 4;
  private static final int VERSION_INDEX = 5;
  private static final int VALUE_INDEX = 6;

  public BinlogRow() {
    super(ROW_SIZE);
  }

  public void setDatabase(String db) {
    setField(DATABASE_INDEX, db);
  }

  public String getDatabase() {
    return getString(DATABASE_INDEX);
  }

  public void setTable(String table) {
    setField(TABLE_INDEX, table);
  }

  public String getTable() {
    return getString(TABLE_INDEX);
  }

  public void setKey(String key) {
    setField(KEY_INDEX, key);
  }

  public String getKey() {
    return getString(KEY_INDEX);
  }

  public void setTimestamp(String ts) {
    setField(TIMESTAMP_INDEX, ts);
  }

  public String getTimestamp() {
    return getString(TIMESTAMP_INDEX);
  }

  public void setDDL(boolean isDDL) {
    setField(DDL_FLAG_INDEX, isDDL);
  }

  public boolean getDDL() {
    return getBoolean(DDL_FLAG_INDEX);
  }

  public void setVersion(int version) {
    setField(VERSION_INDEX, version);
  }

  public int getVersion() {
    return getInt(VERSION_INDEX);
  }

  public void setValue(byte[] value) {
    setField(VALUE_INDEX, value);
  }

  public byte[] getValue() {
    return getBinary(VALUE_INDEX);
  }
}

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

package com.bytedance.bitsail.test.integration.cdc.mysql.context;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;

public class SourceMockContext implements SourceReader.Context {

  private final int id;
  private final RowTypeInfo rowTypeInfo;

  public SourceMockContext(int id, RowTypeInfo rowTypeInfo) {
    this.id = id;
    this.rowTypeInfo = rowTypeInfo;
  }

  @Override
  public RowTypeInfo getRowTypeInfo() {
    return rowTypeInfo;
  }

  @Override
  public int getIndexOfSubtask() {
    return id;
  }

  @Override
  public void sendSplitRequest() {

  }
}
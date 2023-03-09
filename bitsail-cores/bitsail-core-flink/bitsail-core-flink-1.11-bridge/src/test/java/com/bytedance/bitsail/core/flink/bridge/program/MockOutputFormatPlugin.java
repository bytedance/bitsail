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

package com.bytedance.bitsail.core.flink.bridge.program;

import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.flink.core.legacy.connector.OutputFormatPlugin;

import org.apache.flink.types.Row;

import java.io.IOException;

public class MockOutputFormatPlugin extends OutputFormatPlugin<Row> {

  @Override
  public void initPlugin() throws Exception {

  }

  @Override
  public String getType() {
    return MockOutputFormatPlugin.class.getSimpleName();
  }

  @Override
  public void writeRecord(Row o) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void writeRecordInternal(Row record) throws Exception {

  }

  @Override
  public int getMaxParallelism() {
    return 0;
  }

  @Override
  public void tryCleanupOnError() throws Exception {

  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }
}

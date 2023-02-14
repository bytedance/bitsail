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

package com.bytedance.bitsail.connector.kafka.source.reader;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.SourcePipeline;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kafka.source.split.KafkaSplit;

import java.util.List;

public class KafkaSourceReader implements SourceReader<Row, KafkaSplit> {

  public KafkaSourceReader(BitSailConfiguration readerConfiguration,
                           Context context,
                           Boundedness boundedness) {
  }

  @Override
  public void start() {

  }

  @Override
  public void pollNext(SourcePipeline<Row> pipeline) throws Exception {

  }

  @Override
  public void addSplits(List<KafkaSplit> splits) {

  }

  @Override
  public boolean hasMoreElements() {
    return false;
  }

  @Override
  public List<KafkaSplit> snapshotState(long checkpointId) {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}

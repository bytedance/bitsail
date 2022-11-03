/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.kudu.source;

import com.bytedance.bitsail.base.connector.reader.v1.Boundedness;
import com.bytedance.bitsail.base.connector.reader.v1.Source;
import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.kudu.core.KuduConstants;
import com.bytedance.bitsail.connector.kudu.core.KuduFactory;
import com.bytedance.bitsail.connector.kudu.source.split.AbstractKuduSplitConstructor;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSourceSplit;
import com.bytedance.bitsail.connector.kudu.source.split.KuduSplitFactory;

import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.util.List;

public class KuduSource implements Source<Row, KuduSourceSplit, EmptyState> {

  private BitSailConfiguration jobConf;
  private List<KuduSourceSplit> splits;

  @Override
  public void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException {
    this.jobConf = readerConfiguration;

    try (KuduFactory kuduFactory = new KuduFactory(jobConf, "reader")) {
      KuduClient client = kuduFactory.getClient();
      AbstractKuduSplitConstructor splitConstructor = KuduSplitFactory.getSplitConstructor(jobConf, client);
      this.splits = splitConstructor.construct(client);
    }
  }

  @Override
  public Boundedness getSourceBoundedness() {
    return Boundedness.BOUNDEDNESS;
  }

  @Override
  public SourceReader<Row, KuduSourceSplit> createReader(SourceReader.Context readerContext) {
    return new KuduSourceReader(jobConf);
  }

  // todo: 补充完全
  @Override
  public SourceSplitCoordinator<KuduSourceSplit, EmptyState> createSplitCoordinator(SourceSplitCoordinator.Context<KuduSourceSplit, EmptyState> coordinatorContext) {
    return null;
  }

  @Override
  public String getReaderName() {
    return KuduConstants.KUDU_CONNECTOR_NAME;
  }
}

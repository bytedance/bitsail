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

package com.bytedance.bitsail.connector.kudu.source.split;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;

import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

import java.io.IOException;
import java.util.List;

public abstract class AbstractKuduSplitConstructor {

  protected final BitSailConfiguration jobConf;
  protected final String tableName;
  protected final Schema schema;

  public AbstractKuduSplitConstructor(BitSailConfiguration jobConf, KuduClient kuduClient) throws BitSailException {
    this.jobConf = jobConf;
    this.tableName = jobConf.get(KuduReaderOptions.KUDU_TABLE_NAME);
    try {
      this.schema = kuduClient.openTable(tableName).getSchema();
    } catch (KuduException e) {
      throw new BitSailException(KuduErrorCode.OPEN_TABLE_ERROR, "Failed to get schema from table " + tableName);
    }
  }

  public abstract boolean isAvailable();

  protected abstract boolean fillSplitConf(BitSailConfiguration jobConf, KuduClient client) throws IOException;

  public abstract List<KuduSourceSplit> construct(KuduClient kuduClient) throws IOException;

  /**
   * Used for determine parallelism num.
   */
  public abstract int estimateSplitNum() throws IOException;
}

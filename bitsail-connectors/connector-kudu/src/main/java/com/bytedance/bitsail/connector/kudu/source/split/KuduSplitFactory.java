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

package com.bytedance.bitsail.connector.kudu.source.split;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;
import com.bytedance.bitsail.connector.kudu.option.KuduReaderOptions;
import com.bytedance.bitsail.connector.kudu.source.split.strategy.SimpleDivideSplitConstructor;

import org.apache.kudu.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class KuduSplitFactory {
  private static final Logger LOG = LoggerFactory.getLogger(KuduSplitFactory.class);

  public enum KuduSplitStrategy {
    SIMPLE_DIVIDE;
  }

  @SuppressWarnings("checkstyle:FallThrough")
  public static AbstractKuduSplitConstructor getSplitConstructor(BitSailConfiguration jobConf,
                                                                 KuduClient client) {
    KuduSplitStrategy strategy = KuduSplitStrategy.valueOf(jobConf.get(KuduReaderOptions.SPLIT_STRATEGY));
    switch (strategy) {
      case SIMPLE_DIVIDE:
        try {
          return new SimpleDivideSplitConstructor(jobConf, client);
        } catch (IOException e) {
          LOG.warn("Failed to create SimpleDivideSplitConstructor, will try the next constructor type.");
        }
      default:
        throw new BitSailException(KuduErrorCode.SPLIT_ERROR, "Cannot create a split constructor.");
    }
  }
}

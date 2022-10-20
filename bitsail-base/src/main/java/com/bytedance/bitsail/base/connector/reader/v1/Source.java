/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bytedance.bitsail.base.connector.reader.v1;

import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.base.serializer.SimpleBinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.type.BaseEngineTypeInfoConverter;
import com.bytedance.bitsail.common.type.SimpleTypeInfoConverter;

import java.io.Serializable;

public interface Source<T, SplitT extends SourceSplit, StateT extends Serializable> extends Serializable {

  /**
   * Run in client side for source initialize;
   */
  void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration);

  Boundedness getSourceBoundedness();

  SourceReader<T, SplitT> createReader(BitSailConfiguration readerConfiguration,
                                       SourceReader.Context readerContext);

  SourceSplitCoordinator<SplitT, StateT> createSplitCoordinator(BitSailConfiguration readerConfiguration,
                                                                SourceSplitCoordinator.Context<SplitT> coordinatorContext);

  SourceSplitCoordinator<SplitT, StateT> restoreSplitCoordinator(BitSailConfiguration readerConfiguration,
                                                                 SourceSplitCoordinator.Context<SplitT> coordinatorContext,
                                                                 StateT checkpoint);

  default BinarySerializer<SplitT> getSplitSerializer() {
    return new SimpleBinarySerializer<>();
  }

  default BinarySerializer<StateT> getEnumeratorCheckpointSerializer() {
    return new SimpleBinarySerializer<>();
  }

  default BaseEngineTypeInfoConverter createTypeInfoConverter() {
    return new SimpleTypeInfoConverter("bitsail");
  }

  String getReaderName();
}

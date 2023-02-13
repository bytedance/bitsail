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

package com.bytedance.bitsail.base.connector.reader.v1;

import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.base.extension.TypeInfoConverterFactory;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.base.serializer.SimpleVersionedBinarySerializer;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;

import java.io.IOException;
import java.io.Serializable;

public interface Source<T, SplitT extends SourceSplit, StateT extends Serializable>
    extends Serializable, TypeInfoConverterFactory {

  /**
   * Run in client side for source initialize;
   */
  void configure(ExecutionEnviron execution, BitSailConfiguration readerConfiguration) throws IOException;

  /**
   * Indicate the Source type.
   */
  Boundedness getSourceBoundedness();

  /**
   * Create Source Reader.
   */
  SourceReader<T, SplitT> createReader(SourceReader.Context readerContext);

  /**
   * Create split coordinator.
   */
  SourceSplitCoordinator<SplitT, StateT> createSplitCoordinator(SourceSplitCoordinator.Context<SplitT, StateT> coordinatorContext);

  /**
   * Get Split serializer for the framework,{@link SplitT}should implement from {@link  Serializable}
   */
  default BinarySerializer<SplitT> getSplitSerializer() {
    return new SimpleVersionedBinarySerializer<>();
  }

  /**
   * Get State serializer for the framework, {@link StateT}should implement from {@link  Serializable}
   */
  default BinarySerializer<StateT> getSplitCoordinatorCheckpointSerializer() {
    return new SimpleVersionedBinarySerializer<>();
  }

  /**
   * Create type info converter for the source, default value {@link BitSailTypeInfoConverter}
   */
  default TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  /**
   * Get Source' name.
   */
  String getReaderName();
}

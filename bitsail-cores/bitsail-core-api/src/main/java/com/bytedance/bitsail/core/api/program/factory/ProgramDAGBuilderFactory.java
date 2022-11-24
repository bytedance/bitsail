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

package com.bytedance.bitsail.core.api.program.factory;

import com.bytedance.bitsail.base.connector.reader.DataReaderDAGBuilder;
import com.bytedance.bitsail.base.connector.transformer.DataTransformDAGBuilder;
import com.bytedance.bitsail.base.connector.writer.DataWriterDAGBuilder;
import com.bytedance.bitsail.base.execution.Mode;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import java.io.Serializable;
import java.util.List;

public interface ProgramDAGBuilderFactory extends Serializable {

  List<DataReaderDAGBuilder> getDataReaderDAGBuilders(Mode mode,
                                                      List<BitSailConfiguration> readerConfigurations,
                                                      PluginFinder pluginFinder);

  List<DataWriterDAGBuilder> getDataWriterDAGBuilders(Mode mode,
                                                      List<BitSailConfiguration> writerConfigurations,
                                                      PluginFinder pluginFinder);

  /**
   * todo
   */
  default List<DataTransformDAGBuilder> getDataTransformDAGBuilders(Mode mode,
                                                                    List<BitSailConfiguration> writerConfigurations,
                                                                    PluginFinder pluginFinder) {
    throw new UnsupportedOperationException();
  }
}
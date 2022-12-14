/*
 * Copyright 2022 Bytedance and/or its affiliates.
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

package com.bytedance.bitsail.connector.legacy.hudi.sink.append;

import com.bytedance.bitsail.connector.legacy.hudi.sink.common.AbstractWriteOperator;
import com.bytedance.bitsail.connector.legacy.hudi.sink.common.WriteOperatorFactory;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;

/**
 * Operator for {@link AppendWriteFunction}.
 *
 * @param <I> The input type
 */
public class AppendWriteOperator<I> extends AbstractWriteOperator<I> {

  public AppendWriteOperator(Configuration conf, RowType rowType) {
    super(new AppendWriteFunction<>(conf, rowType));
  }

  public static <I> WriteOperatorFactory<I> getFactory(Configuration conf, RowType rowType) {
    return WriteOperatorFactory.instance(conf, new AppendWriteOperator<>(conf, rowType));
  }
}

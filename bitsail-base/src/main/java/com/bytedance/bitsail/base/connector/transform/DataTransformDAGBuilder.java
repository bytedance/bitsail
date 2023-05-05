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

package com.bytedance.bitsail.base.connector.transform;

import com.bytedance.bitsail.base.execution.ExecutionEnviron;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import java.io.Serializable;

/**
 * Created 2022/4/22
 */
public interface DataTransformDAGBuilder extends Serializable {

  /**
   * Initialize {@link DataTransformDAGBuilder} with configurations in execution environment.
   *
   * @param execution           Current execution environment.
   * @param transformConfiguration Configuration for this transform.
   */
  default void configure(ExecutionEnviron execution,
                         BitSailConfiguration transformConfiguration) throws Exception {
  }

  /**
   * Run the validation process before submitting the job.
   */
  default boolean validate() throws Exception {
    return true;
  }

  /**
   * @return The name of transform operator.
   */
  String getTransformName();
}

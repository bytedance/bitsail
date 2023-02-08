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

package com.bytedance.bitsail.component.metrics.prometheus.option;

import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.common.option.ConfigOptions;

public interface PrometheusOptions extends CommonOptions {
  String PROMETHEUS_PREFIX = CommonOptions.COMMON_PREFIX + "metric.prometheus_";

  /**
   * Prometheus port
   */
  ConfigOption<Integer> PROMETHEUS_PORT =
      ConfigOptions.key(PROMETHEUS_PREFIX + "port")
          .defaultValue(9999);;
}

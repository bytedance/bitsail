/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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
import com.bytedance.bitsail.common.option.WriterOptions;

public interface PrometheusPushGatewayOptions extends WriterOptions.BaseWriterOptions {
  String PUSHGATEWAY_PREFIX = CommonOptions.COMMON_PREFIX + "pushgateway-";

  ConfigOption<String> PUSHGATEWAY_HOST_NAME =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "host")
          .defaultValue("localhost");

  ConfigOption<Integer> PUSHGATEWAY_PORT_NUM =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "port")
          .defaultValue(9091);

  ConfigOption<Integer> PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "report-period-seconds")
          .defaultValue(1);

  ConfigOption<Boolean> PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "delete-on-shutdown")
          .defaultValue(true);

  ConfigOption<String> PUSHGATEWAY_JOBNAME =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "jobname")
          .noDefaultValue(String.class);
}

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

public interface PrometheusPushGatewayOptions extends CommonOptions {
  String PUSHGATEWAY_PREFIX = CommonOptions.COMMON_PREFIX + "metric.pushgateway_";

  ConfigOption<String> PUSHGATEWAY_HOST_NAME =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "host")
          .defaultValue("localhost");

  ConfigOption<Integer> PUSHGATEWAY_PORT =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "port")
          .defaultValue(9091);

  ConfigOption<Integer> PUSHGATEWAY_HTTPS_PORT =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "https_port")
          .defaultValue(443);

  ConfigOption<Integer> PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "report_period_seconds")
          .defaultValue(1);

  ConfigOption<Boolean> PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "delete_on_shutdown")
          .defaultValue(true);

  ConfigOption<String> PUSHGATEWAY_JOBNAME =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "jobname")
          .noDefaultValue(String.class);

  ConfigOption<String> PUSHGATEWAY_DEFAULT_JOBNAME_SUFFIX =
      ConfigOptions.key(PUSHGATEWAY_PREFIX + "default_jobName_suffix")
          .defaultValue("_metric");
}

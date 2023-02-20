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

package com.bytedance.bitsail.core.api.parser;

import com.bytedance.bitsail.base.component.DefaultComponentBuilderLoader;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.core.api.command.CoreCommandArgs;
import com.bytedance.bitsail.core.api.interceptor.ConfigInterceptorHelper;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

public class ConfigurationHelper {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigurationHelper.class);

  public static BitSailConfiguration load(CoreCommandArgs coreCommandArgs) {
    DefaultComponentBuilderLoader<ConfigurationParser> loader =
        new DefaultComponentBuilderLoader<>(ConfigurationParser.class);

    List<ConfigurationParser> parsers = Lists.newArrayList(loader.loadComponents());
    BitSailConfiguration configuration = null;
    try {
      for (ConfigurationParser parser : parsers) {
        if (parser.accept(coreCommandArgs)) {
          LOG.info("Config parser: {} accepted the arguments.", parser.getComponentName());
          configuration = parser.parse(coreCommandArgs);
          break;
        }
      }
    } catch (Exception e) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR, e);
    }
    if (Objects.isNull(configuration)) {
      throw BitSailException.asBitSailException(CommonErrorCode.INTERNAL_ERROR,
          "Configuration can't be null, plz check your input.");
    }
    ConfigInterceptorHelper.intercept(configuration, coreCommandArgs);

    return configuration;
  }

}

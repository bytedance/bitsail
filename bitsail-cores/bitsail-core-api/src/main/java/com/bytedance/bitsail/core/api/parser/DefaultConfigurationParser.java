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

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.configuration.ConfigParser;
import com.bytedance.bitsail.core.api.command.CoreCommandArgs;

import org.apache.commons.lang3.StringUtils;

import java.util.Base64;

public class DefaultConfigurationParser implements ConfigurationParser {
  @Override
  public String getComponentName() {
    return "default";
  }

  @Override
  public boolean accept(CoreCommandArgs coreCommandArgs) {
    return StringUtils.isNotEmpty(coreCommandArgs.getJobConfPath())
        || StringUtils.isNotEmpty(coreCommandArgs.getJobConfBase64());
  }

  @Override
  public BitSailConfiguration parse(CoreCommandArgs coreCommandArgs) {
    if (StringUtils.isNotEmpty(coreCommandArgs.getJobConfPath())) {
      return ConfigParser.fromRawConfPath(coreCommandArgs.getJobConfPath());
    } else {
      return BitSailConfiguration.from(
          new String(Base64.getDecoder().decode(coreCommandArgs.getJobConfBase64())));
    }
  }
}

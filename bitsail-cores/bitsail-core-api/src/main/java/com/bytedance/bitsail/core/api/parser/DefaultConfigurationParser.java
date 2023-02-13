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

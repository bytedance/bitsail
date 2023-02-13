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

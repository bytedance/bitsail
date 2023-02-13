package com.bytedance.bitsail.core.api.parser;

import com.bytedance.bitsail.base.extension.Component;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.core.api.command.CoreCommandArgs;

import java.io.IOException;

public interface ConfigurationParser extends Component {

  /**
   * Accept command or not.
   */
  boolean accept(CoreCommandArgs args);

  BitSailConfiguration parse(CoreCommandArgs args) throws IOException;
}

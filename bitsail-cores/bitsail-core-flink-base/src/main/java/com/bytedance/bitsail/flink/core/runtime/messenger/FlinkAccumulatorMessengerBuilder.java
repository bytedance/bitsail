/*
 * Copyright 2022-present ByteDance.
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

package com.bytedance.bitsail.flink.core.runtime.messenger;

import com.bytedance.bitsail.base.messenger.Messenger;
import com.bytedance.bitsail.base.messenger.MessengerBuilder;
import com.bytedance.bitsail.base.messenger.context.MessengerContext;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.flink.core.runtime.messenger.impl.FlinkAccumulatorStatisticsMessenger;

/**
 * Created 2022/8/31
 */
public class FlinkAccumulatorMessengerBuilder implements MessengerBuilder {

  @Override
  public String getComponentName() {
    return "flink";
  }

  @Override
  public Messenger createMessenger(MessengerContext messengerContext,
                                   BitSailConfiguration commonConfiguration) {
    return new FlinkAccumulatorStatisticsMessenger(messengerContext, commonConfiguration);
  }
}

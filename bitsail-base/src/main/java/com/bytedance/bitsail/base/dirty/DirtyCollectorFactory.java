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

package com.bytedance.bitsail.base.dirty;

import com.bytedance.bitsail.base.component.DefaultComponentBuilderLoader;
import com.bytedance.bitsail.base.messenger.context.MessengerContext;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;

public class DirtyCollectorFactory {
  public static AbstractDirtyCollector initDirtyCollector(BitSailConfiguration commonConfiguration,
                                                          MessengerContext messengerContext) {
    String dirtyCollectorType = commonConfiguration.get(
        CommonOptions.MESSENGER_COLLECTOR_TYPE);

    DefaultComponentBuilderLoader<DirtyCollectorBuilder> componentLoader =
        new DefaultComponentBuilderLoader<>(DirtyCollectorBuilder.class);

    DirtyCollectorBuilder dirtyCollectorBuilder = componentLoader.loadComponent(dirtyCollectorType);

    return dirtyCollectorBuilder.createDirtyCollector(commonConfiguration, messengerContext);
  }

}

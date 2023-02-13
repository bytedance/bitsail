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

package com.bytedance.bitsail.core.api.interceptor;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ConfigOption;
import com.bytedance.bitsail.core.api.command.CoreCommandArgs;

public class FakeConfigInterceptor implements ConfigInterceptor {
  private static final ConfigOption<String> TEST_KEY = CommonOptions.JOB_TYPE;
  private static final String TEST_VAL = "test_type";

  @Override
  public boolean accept(BitSailConfiguration globalConfiguration) {
    return true;
  }

  @Override
  public void intercept(BitSailConfiguration globalConfiguration, CoreCommandArgs coreCommandArgs) {
    globalConfiguration.set(TEST_KEY, TEST_VAL);
  }

  @Override
  public String getComponentName() {
    return "fake-config-interceptor";
  }

  public static boolean validate(BitSailConfiguration globalConfiguration) {
    return globalConfiguration.fieldExists(TEST_KEY)
        && TEST_VAL.equals(globalConfiguration.get(TEST_KEY));
  }
}

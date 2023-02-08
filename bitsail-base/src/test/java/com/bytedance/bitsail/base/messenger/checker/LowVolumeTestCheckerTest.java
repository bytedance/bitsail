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

package com.bytedance.bitsail.base.messenger.checker;

import com.bytedance.bitsail.base.messenger.BaseStatisticsMessenger;
import com.bytedance.bitsail.base.messenger.Messenger;
import com.bytedance.bitsail.base.messenger.common.MessengerGroup;
import com.bytedance.bitsail.base.messenger.context.SimpleMessengerContext;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;

import com.google.common.collect.ImmutableMap;
import lombok.val;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class LowVolumeTestCheckerTest {

  private Messenger messenger;

  @Before
  public void before() {
    messenger = new BaseStatisticsMessenger(SimpleMessengerContext
        .builder()
        .messengerGroup(MessengerGroup.READER)
        .build());
    messenger.open();
  }

  @Test
  public void check_noThreshold() {
    val checker = new LowVolumeTestChecker(BitSailConfiguration.from(ImmutableMap.of()));

    assertFalse(checker.check(messenger.getSuccessRecords(), messenger.getFailedRecords()));
  }

  @Test
  public void check_breachThreshold() {
    val checker = new LowVolumeTestChecker(BitSailConfiguration.newDefault()
        .set(CommonOptions.LOW_VOLUME_TEST_COUNT_THRESHOLD, 10L));

    assertFalse(checker.check(messenger.getSuccessRecords(), messenger.getFailedRecords()));
  }

  @Test
  public void restrictSplitsNumber() {
    val checker = new LowVolumeTestChecker(BitSailConfiguration.newDefault()
        .set(CommonOptions.LOW_VOLUME_TEST_COUNT_THRESHOLD, 10L));

    assertEquals(100, checker.restrictSplitsNumber(new String[200]).length);
  }
}
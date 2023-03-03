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

package com.bytedance.bitsail.test.integration.legacy.larksheet;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.larksheet.option.LarkSheetReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.Ignore;
import org.junit.Test;

@Ignore("Online lark sheet is not stable")
public class LarkSheetSourceITCase extends AbstractIntegrationTest {

  @Test
  public void testLarkSheetToPrint() throws Exception {
    BitSailConfiguration conf = JobConfUtils.fromClasspath("larksheet_to_print.json");
    addAppInfo(conf);
    submitJob(conf);
  }

  // todo: add an available app for test
  private void addAppInfo(BitSailConfiguration conf) {
    conf.set(LarkSheetReaderOptions.APP_ID, "test_app_id");
    conf.set(LarkSheetReaderOptions.APP_SECRET, "test_app_secret");
  }
}

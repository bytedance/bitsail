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

package com.bytedance.bitsail.test.integration.localfilesystem;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.Test;

public class LocalFileSystemSourceITCase extends AbstractIntegrationTest {

  @Test
  public void testUnifiedLocalFSSourceWithCSV() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("scripts/local_file_to_print_csv.json");
    submitJob(jobConf);
  }

  @Test
  public void testUnifiedLocalFSSourceWithJSON() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("scripts/local_file_to_print_json.json");
    submitJob(jobConf);
  }
}

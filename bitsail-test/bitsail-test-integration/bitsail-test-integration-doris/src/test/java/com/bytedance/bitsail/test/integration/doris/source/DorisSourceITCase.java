/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.test.integration.doris.source;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.doris.option.DorisReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class DorisSourceITCase extends AbstractIntegrationTest {

  @Test
  public void test() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("doris_to_print.json");
    addDorisInfo(jobConf);
    submitJob(jobConf);
  }

  /**
   * Add your doris setting to job configuration.
   * Below codes are just example.
   * <p>
   * select id, bigint_type, string_type, double_type from doris_table where id = 1
   */
  public void addDorisInfo(BitSailConfiguration jobConf) {
    jobConf.set(DorisReaderOptions.FE_HOSTS, "127.0.0.1:8030");
    jobConf.set(DorisReaderOptions.MYSQL_HOSTS, "127.0.0.1:9030");
    jobConf.set(DorisReaderOptions.USER, "root");
    jobConf.set(DorisReaderOptions.PASSWORD, "");
    jobConf.set(DorisReaderOptions.DB_NAME, "test");
    jobConf.set(DorisReaderOptions.TABLE_NAME, "test_bitsail");
//    jobConf.set(DorisReaderOptions.SQL_FILTER, "id=1");
  }

}

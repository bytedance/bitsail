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

package com.bytedance.bitsail.connector.elasticsearch.utils;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class SplitStringUtilsTest {

  private static final Logger LOG = LoggerFactory.getLogger(SplitStringUtilsTest.class);

  @Test
  public void testSplitString() {
    String[] splitNames = SplitStringUtils.splitString(" test1,  test2, , test3 , test4  ");
    LOG.info("split names: {}", Arrays.toString(splitNames));
    Assert.assertEquals("Index names parse error.", 4, splitNames.length);
  }
}

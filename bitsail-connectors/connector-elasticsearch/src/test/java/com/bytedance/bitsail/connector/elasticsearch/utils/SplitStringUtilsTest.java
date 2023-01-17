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

package com.bytedance.bitsail.conector.legacy.fake.source;

import com.bytedance.bitsail.connector.legacy.fake.source.FakeSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class FakeSourceTest {

  @Test
  public void testEmptyUniqueFieldsMapping() {
    Assert.assertTrue(FakeSource.initUniqueFieldsMapping(null).isEmpty());
    Assert.assertTrue(FakeSource.initUniqueFieldsMapping("   ").isEmpty());
  }

  @Test
  public void testUniqueFieldsMapping() {
    Map<String, Set<String>> fieldMapping = FakeSource.initUniqueFieldsMapping("id,date");
    Assert.assertEquals(2, fieldMapping.size());
    Assert.assertTrue(fieldMapping.containsKey("id"));
    Assert.assertTrue(fieldMapping.containsKey("date"));
  }

  @Test
  public void testConstructRandomValueWithoutUniqueCheck() {
    long expectValue = 1234L;
    long actualValue = FakeSource.constructRandomValue(null, () -> expectValue);
    Assert.assertEquals(expectValue, actualValue);
  }

  @Test
  public void testConstructRandomValueWithUniqueCheck() {
    AtomicInteger constructCount = new AtomicInteger(0);
    Set<String> existValues = new HashSet<>();
    existValues.add("1234");

    long actualValue = FakeSource.constructRandomValue(existValues,
      () -> (constructCount.getAndIncrement() == 0) ? 1234L : 5678);

    Assert.assertEquals(5678, actualValue);
    Assert.assertTrue(existValues.contains("5678"));
  }
}

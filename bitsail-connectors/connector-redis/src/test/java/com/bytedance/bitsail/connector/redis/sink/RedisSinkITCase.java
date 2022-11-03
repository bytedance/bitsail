package com.bytedance.bitsail.connector.redis.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.connector.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.redis.option.RedisWriterOptions;
import com.bytedance.bitsail.test.connector.test.EmbeddedFlinkCluster;
import com.bytedance.bitsail.test.connector.test.testcontainers.redis.RedisContainer;
import com.bytedance.bitsail.test.connector.test.utils.JobConfUtils;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RedisSinkITCase {
  private static final int TOTAL_COUNT = 300;
  private RedisContainer redisContainer;
  private String redisHost;
  private int redisPort;

  @Before
  public void initRedis() {
    redisContainer = new RedisContainer();
    redisContainer.start();
    redisHost = redisContainer.getHost();
    redisPort = redisContainer.getPort();
  }
  @After
  public void closeRedis() throws Exception {
    redisContainer.close();
  }

  @Test
  public void testBatch() throws Exception {
    BitSailConfiguration jobConfiguration = JobConfUtils.fromClasspath("fake_to_redis.json");
    jobConfiguration.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConfiguration.set(CommonOptions.JOB_TYPE, "BATCH");
    jobConfiguration.set(FakeReaderOptions.RATE, 1000);
    jobConfiguration.set(RedisWriterOptions.HOST, redisHost);
    jobConfiguration.set(RedisWriterOptions.PORT, redisPort);

    EmbeddedFlinkCluster.submitJob(jobConfiguration);

    Assert.assertEquals(TOTAL_COUNT, redisContainer.getKeyCount());
  }


  @Test
  public void testStreaming() throws Exception {
    BitSailConfiguration jobConfiguration = JobConfUtils.fromClasspath("fake_to_redis.json");
    jobConfiguration.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConfiguration.set(CommonOptions.JOB_TYPE, "STREAMING");
    jobConfiguration.set(CommonOptions.CheckPointOptions.CHECKPOINT_ENABLE, true);
    jobConfiguration.set(RedisWriterOptions.HOST, redisHost);
    jobConfiguration.set(RedisWriterOptions.PORT, redisPort);

    EmbeddedFlinkCluster.submitJob(jobConfiguration);

    Assert.assertEquals(TOTAL_COUNT, redisContainer.getKeyCount());
  }

}

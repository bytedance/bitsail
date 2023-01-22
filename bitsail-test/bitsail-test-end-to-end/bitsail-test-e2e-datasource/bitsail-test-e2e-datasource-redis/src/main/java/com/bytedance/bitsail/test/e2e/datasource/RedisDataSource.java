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

package com.bytedance.bitsail.test.e2e.datasource;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.connector.legacy.redis.option.RedisWriterOptions;
import com.bytedance.bitsail.connector.redis.sink.RedisSink;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class RedisDataSource extends AbstractDataSource {
  private static final Logger LOG = LoggerFactory.getLogger(RedisDataSource.class);

  private static final String REDIS_VERSION =  "redis:6.2.6";
  private static final int DEFAULT_EXPOSED_PORT = 6379;

  private GenericContainer<?> redis;
  private RedisBackedCache backedCache;

  private String host;
  private int port;

  @Override
  public String getContainerName() {
    return "data-source:redis";
  }

  @Override
  public void initNetwork(Network executorNetwork) {
    this.network = executorNetwork;
  }

  @Override
  public void configure(BitSailConfiguration dataSourceConf) {

  }

  @Override
  public boolean accept(BitSailConfiguration jobConf, String sourceType) {
    if ("redis".equalsIgnoreCase(sourceType)) {
      return true;
    }
    String writerClass = jobConf.get(ReaderOptions.READER_CLASS);
    return RedisSink.class.getName().equals(writerClass);
  }

  @Override
  public void modifyJobConf(BitSailConfiguration jobConf) {
    jobConf.set(RedisWriterOptions.HOST, host);
    jobConf.set(RedisWriterOptions.PORT, port);
  }

  @Override
  public void start() {
    redis = new GenericContainer<>(DockerImageName.parse(REDIS_VERSION))
        .withExposedPorts(DEFAULT_EXPOSED_PORT);
    redis.start();
    this.host = redis.getHost();
    this.port = redis.getFirstMappedPort();
    LOG.info("Redis container starts! Host is: [{}], port is: [{}].", host, port);
  }

  @Override
  public void fillData() {

  }

  @Override
  public void validate() throws BitSailException {
    // todo: add validation here.
  }

  @Override
  public void close() throws IOException {
    if (backedCache != null) {
      backedCache.close();
      backedCache = null;
    }
    redis.close();
    LOG.info("Redis container closed.");

    super.close();
  }

  /**
   * Only support searching kv.
   */
  public String getKey(String key) {
    if (Objects.isNull(backedCache)) {
      backedCache = new RedisBackedCache(host, port);
    }
    return backedCache.get(key);
  }

  /**
   * Count keys.
   */
  public int getKeyCount() {
    if (Objects.isNull(backedCache)) {
      backedCache = new RedisBackedCache(host, port);
    }
    List<String> keys = backedCache.getAllKeys();
    LOG.info("Get {} keys from redis.", keys.size());
    return keys.size();
  }

  /**
   * Redis backed cache.
   */
  static class RedisBackedCache implements Closeable {
    private final RedisClient client;
    private final StatefulRedisConnection<String, String> connection;

    public RedisBackedCache(String host, int port) {
      client = RedisClient.create(String.format("redis://%s:%d/0", host, port));
      connection = client.connect();
    }

    public String get(String key) {
      return connection.sync().get(key);
    }

    public List<String> getAllKeys() {
      return connection.sync().keys("*");
    }

    @Override
    public void close() {
      connection.close();
      client.close();
    }
  }
}

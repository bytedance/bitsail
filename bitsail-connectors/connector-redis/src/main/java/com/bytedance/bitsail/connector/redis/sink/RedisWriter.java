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

package com.bytedance.bitsail.connector.redis.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.connector.redis.core.TtlType;
import com.bytedance.bitsail.connector.redis.core.api.PipelineProcessor;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommand;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisDataType;
import com.bytedance.bitsail.connector.redis.error.RedisPluginErrorCode;
import com.bytedance.bitsail.connector.redis.error.RedisUnexpectedException;
import com.bytedance.bitsail.connector.redis.option.RedisWriterOptions;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RedisWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {

  /**
   * Jedis connection pool.
   */
  private final JedisPool jedisPool;

  /**
   * Retryer for obtaining jedis.
   */
  private final Retryer.RetryerCallable<Jedis> jedisFetcher;

  private final Retryer<Boolean> retryer;

  private final CircularFifoQueue<Row> recordQueue;

  /**
   * pipeline id for logging.
   */
  private long processorId;

  /**
   * Command used in the job.
   */
  private final JedisCommand jedisCommand;

  /**
   * Complex type command with ttl.
   */
  private boolean complexTypeWithTtl;

  /**
   * Log interval of pipelines.
   */
  private final int logSampleInterval;

  /**
   * Retryer retry count
   */
  private final int maxAttemptCount;

  @SuppressWarnings("checkstyle:MagicNumber")
  public RedisWriter(BitSailConfiguration writerConfiguration, Context<EmptyState> context) {
    // initialize ttl
    int ttl = writerConfiguration.getUnNecessaryOption(RedisWriterOptions.TTL, -1);
    TtlType ttlType;
    try {
      ttlType = TtlType.valueOf(StringUtils.upperCase(writerConfiguration.get(RedisWriterOptions.TTL_TYPE)));
    } catch (IllegalArgumentException e) {
      throw BitSailException.asBitSailException(RedisPluginErrorCode.ILLEGAL_VALUE,
          String.format("unknown ttl type: %s", writerConfiguration.get(RedisWriterOptions.TTL_TYPE)));
    }
    int ttlInSeconds = ttl < 0 ? -1 : ttl * ttlType.getContainSeconds();
    log.info("ttl is {}(s)", ttlInSeconds);

    // initialize command factory
    String redisDataType = StringUtils.upperCase(writerConfiguration.get(RedisWriterOptions.REDIS_DATA_TYPE));
    String additionalKey = writerConfiguration.getUnNecessaryOption(RedisWriterOptions.ADDITIONAL_KEY, "default_redis_key");
    this.jedisCommand = initJedisCommand(redisDataType, ttlInSeconds, additionalKey, context.getRowTypeInfo().getTypeInfos());

    // initialize jedis pool
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_TOTAL_CONNECTIONS));
    jedisPoolConfig.setMaxIdle(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_IDLE_CONNECTIONS));
    jedisPoolConfig.setMinIdle(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MIN_IDLE_CONNECTIONS));
    jedisPoolConfig.setMaxWait(Duration.ofMillis(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_WAIT_TIME_IN_MILLIS)));

    String redisHost = writerConfiguration.getNecessaryOption(RedisWriterOptions.HOST, RedisPluginErrorCode.REQUIRED_VALUE);
    int redisPort = writerConfiguration.getNecessaryOption(RedisWriterOptions.PORT, RedisPluginErrorCode.REQUIRED_VALUE);
    String redisPassword = writerConfiguration.get(RedisWriterOptions.PASSWORD);
    int timeout = writerConfiguration.get(RedisWriterOptions.CLIENT_TIMEOUT_MS);

    if (StringUtils.isEmpty(redisPassword)) {
      this.jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout);
    } else {
      this.jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout, redisPassword);
    }

    // initialize record queue
    int batchSize = writerConfiguration.get(RedisWriterOptions.WRITE_BATCH_SIZE);
    this.recordQueue = new CircularFifoQueue<>(batchSize);

    this.logSampleInterval = writerConfiguration.get(RedisWriterOptions.LOG_SAMPLE_INTERVAL);
    this.jedisFetcher = RetryerBuilder.<Jedis>newBuilder()
        .retryIfResult(Objects::isNull)
        .retryIfRuntimeException()
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .withWaitStrategy(WaitStrategies.exponentialWait(100, 5, TimeUnit.MINUTES))
        .build()
        .wrap(jedisPool::getResource);

    this.maxAttemptCount = writerConfiguration.get(RedisWriterOptions.MAX_ATTEMPT_COUNT);
    this.retryer = RetryerBuilder.<Boolean>newBuilder()
        .retryIfResult(needRetry -> Objects.equals(needRetry, true))
        .retryIfException(e -> !(e instanceof BitSailException))
        .withWaitStrategy(WaitStrategies.fixedWait(3, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(maxAttemptCount))
        .build();
  }

  private JedisCommand initJedisCommand(String redisDataType, int ttlSeconds, String additionalKey, TypeInfo<?>[] typeInfos) {
    JedisDataType dataType = JedisDataType.valueOf(redisDataType.toUpperCase());
    JedisCommand jedisCommand;
    this.complexTypeWithTtl = ttlSeconds > 0;
    switch (dataType) {
      case STRING:
        jedisCommand = JedisCommand.SET;
        if (ttlSeconds > 0) {
          jedisCommand = JedisCommand.SETEX;
          this.complexTypeWithTtl = false;
        }
        break;
      case SET:
        jedisCommand = JedisCommand.SADD;
        break;
      case HASH:
        jedisCommand = JedisCommand.HSET;
        break;
      case MHASH:
        jedisCommand = JedisCommand.HMSET;
        break;
      case SORTED_SET:
        jedisCommand = JedisCommand.ZADD;
        break;
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR, "The configure date type " + redisDataType +
            " is not supported, only support string, set, hash, multi-hash, sorted set.");
    }
    if (ttlSeconds <= 0) {
      return jedisCommand.initialize(additionalKey, null, typeInfos);
    }
    return jedisCommand.initialize(additionalKey, ttlSeconds, typeInfos);
  }

  @Override
  public void write(Row record) throws IOException {
    validate(record);
    this.recordQueue.add(record);
    if (recordQueue.isAtFullCapacity()) {
      flush(false);
    }
  }

  /**
   * Pre-check data.
   */
  private void validate(Row record) throws BitSailException {
    for (int i = 0; i < record.getArity(); i++) {
      if (record.getField(i) == null) {
        throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_ENCODING,
            String.format("record contains null element in index[%d]", i));
      }
    }
  }

  @Override
  @SneakyThrows
  public void flush(boolean endOfInput) {
    processorId++;
    try (PipelineProcessor processor = genPipelineProcessor(recordQueue.size(), this.complexTypeWithTtl)) {
      Row record;
      while ((record = recordQueue.poll()) != null) {
        processor.addInitialCommand(jedisCommand.applyCommand(record));
      }
      retryer.call(processor::run);
    } catch (ExecutionException | RetryException e) {
      if (e.getCause() instanceof BitSailException) {
        throw (BitSailException) e.getCause();
      } else if (e.getCause() instanceof RedisUnexpectedException) {
        throw (RedisUnexpectedException) e.getCause();
      }
      throw e;
    } catch (IOException e) {
      throw new RuntimeException("Error while init jedis client.", e);
    }
  }

  /**
   * Gen pipeline processor for jedis commands.
   *
   * @return Jedis pipeline.
   */
  private PipelineProcessor genPipelineProcessor(int commandSize, boolean complexTypeWithTtl) throws ExecutionException, RetryException {
    return new RedisPipelineProcessor(jedisPool, jedisFetcher, commandSize, processorId, logSampleInterval, complexTypeWithTtl, maxAttemptCount);
  }

  @Override
  public List<CommitT> prepareCommit() {
    return Collections.emptyList();
  }

  @Override
  public void close() throws IOException {
    Writer.super.close();
    jedisPool.close();
  }
}

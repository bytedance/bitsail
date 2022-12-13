/*
 * Copyright [2022] [ByteDance]
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
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.redis.config.JedisPoolOptions;
import com.bytedance.bitsail.connector.redis.config.RedisOptions;
import com.bytedance.bitsail.connector.redis.core.Command;
import com.bytedance.bitsail.connector.redis.core.api.PipelineProcessor;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommand;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;
import com.bytedance.bitsail.connector.redis.error.RedisUnexpectedException;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import lombok.SneakyThrows;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import static com.bytedance.bitsail.connector.redis.constant.RedisConstants.SORTED_SET_OR_HASH_COLUMN_SIZE;

public class RedisWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {

  private static final Logger LOG = LoggerFactory.getLogger(RedisWriter.class);

  /**
   * Jedis connection pool.
   */
  private transient JedisPool jedisPool;

  /**
   * Retryer for obtaining jedis.
   */
  private transient Retryer.RetryerCallable<Jedis> jedisFetcher;

  private transient Retryer<Boolean> retryer;

  private transient CircularFifoQueue<Row> recordQueue;

  /**
   * pipeline id for logging.
   */
  private long processorId;

  /**
   * Command used in the job.
   */
  private JedisCommandDescription commandDescription;

  /**
   * Number of columns to send in each record.
   */
  private int columnSize;

  /**
   * Complex type command with ttl.
   */
  private boolean complexTypeWithTtl;

  /**
   * Log interval of pipelines.
   */
  private int logSampleInterval;

  /**
   * Retryer retry count
   */
  private int maxAttemptCount;

  @SuppressWarnings("checkstyle:MagicNumber")
  public RedisWriter(RedisOptions redisOptions, JedisPoolOptions jedisPoolOptions) {
    this.recordQueue = new CircularFifoQueue<>(redisOptions.getBatchInterval());
    this.columnSize = redisOptions.getColumnSize();
    this.complexTypeWithTtl = redisOptions.isComplexTypeWithTtl();
    this.commandDescription = redisOptions.getCommandDescription();
    this.logSampleInterval = redisOptions.getLogSampleInterval();
    this.maxAttemptCount = redisOptions.getMaxAttemptCount();

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(jedisPoolOptions.getMaxTotalConnection());
    jedisPoolConfig.setMaxIdle(jedisPoolOptions.getMaxIdleConnection());
    jedisPoolConfig.setMinIdle(jedisPoolOptions.getMinIdleConnection());
    jedisPoolConfig.setMaxWait(Duration.ofMillis(jedisPoolOptions.getMaxWaitTimeInMillis()));

    String redisPassword = redisOptions.getRedisPassword();
    String redisHost = redisOptions.getRedisHost();
    int redisPort = redisOptions.getRedisPort();
    int timeout = redisOptions.getTimeout();
    if (StringUtils.isEmpty(redisPassword)) {
      this.jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout);
    } else {
      this.jedisPool = new JedisPool(jedisPoolConfig, redisHost, redisPort, timeout, redisPassword);
    }

    this.jedisFetcher = RetryerBuilder.<Jedis>newBuilder()
        .retryIfResult(Objects::isNull)
        .retryIfRuntimeException()
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .withWaitStrategy(WaitStrategies.exponentialWait(100, 5, TimeUnit.MINUTES))
        .build()
        .wrap(jedisPool::getResource);
    this.retryer = RetryerBuilder.<Boolean>newBuilder()
        .retryIfResult(needRetry -> Objects.equals(needRetry, true))
        .retryIfException(e -> !(e instanceof BitSailException))
        .withWaitStrategy(WaitStrategies.fixedWait(3, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(maxAttemptCount))
        .build();
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
    if (record.getArity() != columnSize) {
      throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR,
          String.format("The record's size is %d , but supposed to be %d!", record.getArity(), columnSize));
    }
    for (int i = 0; i < columnSize; i++) {
      if (record.getField(i) == null) {
        throw BitSailException.asBitSailException(CommonErrorCode.UNSUPPORTED_ENCODING,
            String.format("record contains null element in index[%d]", i));
      }
    }
    if (commandDescription.getJedisCommand() == JedisCommand.ZADD) {
      parseScoreFromBytes((byte[]) record.getField(1));
    }
  }

  /**
   * check if score field can be parsed to double.
   */
  private double parseScoreFromBytes(byte[] scoreInBytes) throws BitSailException {
    try {
      return Double.parseDouble(new String(scoreInBytes));
    } catch (NumberFormatException exception) {
      throw new BitSailException(CommonErrorCode.CONVERT_NOT_SUPPORT,
          String.format("The score can't convert to double. And the score is %s.",
              new String(scoreInBytes)));
    }
  }

  @Override
  @SneakyThrows
  public void flush(boolean endOfInput) throws IOException {
    processorId++;
    try (PipelineProcessor processor = genPipelineProcessor(recordQueue.size(), this.complexTypeWithTtl)) {
      Row record;
      while ((record = recordQueue.poll()) != null) {

        byte[] key = ((String) record.getField(0)).getBytes();
        byte[] value = ((String) record.getField(1)).getBytes();
        byte[] scoreOrHashKey = value;
        if (columnSize == SORTED_SET_OR_HASH_COLUMN_SIZE) {
          value = ((String) record.getField(2)).getBytes();
          // Replace empty key with additionalKey in sorted set and hash.
          if (key.length == 0) {
            key = commandDescription.getAdditionalKey().getBytes();
          }
        }

        if (commandDescription.getJedisCommand() == JedisCommand.ZADD) {
          // sortedSet
          processor.addInitialCommand(new Command(commandDescription, key, parseScoreFromBytes(scoreOrHashKey), value));
        } else if (commandDescription.getJedisCommand() == JedisCommand.HSET) {
          // hash
          processor.addInitialCommand(new Command(commandDescription, key, scoreOrHashKey, value));
        } else {
          // set and string
          processor.addInitialCommand(new Command(commandDescription, key, value));
        }
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
    try {
      if (CollectionUtils.isNotEmpty(recordQueue)) {
        flush(true);
      }
    } catch (IOException e) {
      LOG.error("flush the last pipeline occurs error.", e);
      throw e;
    } finally {
      Writer.super.close();
      jedisPool.close();
    }
  }
}

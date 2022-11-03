package com.bytedance.bitsail.connector.redis.sink;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.connector.redis.core.Command;
import com.bytedance.bitsail.connector.redis.core.api.PipelineProcessor;
import com.bytedance.bitsail.connector.redis.error.UnexpectedException;
import com.bytedance.bitsail.connector.redis.config.RedisOptions;
import com.bytedance.bitsail.connector.redis.constant.RedisConstants;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommand;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class RedisWriter<CommitT> implements Writer<Row, CommitT, EmptyState> {
  private static final int SORTED_SET_OR_HASH_COLUMN_SIZE = 3;
  public static final int MAX_ATTEMPT_NUM = 5;
  private static final Logger LOG = LoggerFactory.getLogger(RedisWriter.class);

  /**
   * Jedis connection pool.
   */
  protected transient JedisPool jedisPool;

  /**
   * Retryer for obtaining jedis.
   */
  protected transient Retryer.RetryerCallable<Jedis> jedisFetcher;

  protected transient Retryer<Boolean> retryer;
  protected transient CircularFifoQueue<Row> recordQueue;
  /**
   * pipeline id for logging.
   */
  protected long processorId;

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

  public RedisWriter(RedisOptions redisOptions) {
    this.recordQueue = new CircularFifoQueue<>(redisOptions.getBatchInterval());
    columnSize = redisOptions.getColumnSize();
    complexTypeWithTtl = redisOptions.isComplexTypeWithTtl();
    commandDescription = redisOptions.getCommandDescription();
    logSampleInterval = redisOptions.getLogSampleInterval();

    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(RedisConstants.DEFAULT_MAX_TOTAL_CONNECTIONS);
    jedisPoolConfig.setMaxIdle(RedisConstants.DEFAULT_MAX_IDLE_CONNECTIONS);
    jedisPoolConfig.setMinIdle(RedisConstants.DEFAULT_MIN_IDLE_CONNECTIONS);
    jedisPoolConfig.setMaxWaitMillis(RedisConstants.DEFAULT_MAX_WAIT_TIME_IN_MILLS);

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
        .withStopStrategy(StopStrategies.stopAfterAttempt(MAX_ATTEMPT_NUM))
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
  protected void validate(Row record) throws BitSailException {
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
  @VisibleForTesting
  public double parseScoreFromBytes(byte[] scoreInBytes) throws BitSailException {
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

        //***************** todo: remove this logic *****************
        byte[] key = ((String) record.getField(0)).getBytes();
        byte[] value = ((String) record.getField(1)).getBytes();
        byte[] scoreOrHashKey = value;
        if (columnSize == SORTED_SET_OR_HASH_COLUMN_SIZE) {
          value = (byte[]) record.getField(2);
          // Replace empty key with additionalKey in sorted set and hash.
          if (key.length == 0) {
            key = commandDescription.getAdditionalKey().getBytes();
          }
        }
        //*******************************************************

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
      } else if (e.getCause() instanceof UnexpectedException) {
        throw (UnexpectedException) e.getCause();
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
  protected PipelineProcessor genPipelineProcessor(int commandSize, boolean complexTypeWithTtl) throws ExecutionException, RetryException {
    return new RedisPipelineProcessor(jedisPool, jedisFetcher, commandSize, processorId, logSampleInterval, complexTypeWithTtl);
  }

  @Override
  public List<CommitT> prepareCommit() throws IOException {
    return null;
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
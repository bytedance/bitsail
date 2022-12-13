/*
 * Copyright 2022-present ByteDance.
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

import com.bytedance.bitsail.base.connector.writer.v1.Sink;
import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.base.serializer.BinarySerializer;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.model.ColumnInfo;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.connector.redis.config.JedisPoolOptions;
import com.bytedance.bitsail.connector.redis.config.RedisOptions;
import com.bytedance.bitsail.connector.redis.core.TtlType;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommand;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;
import com.bytedance.bitsail.connector.redis.core.jedis.JedisDataType;
import com.bytedance.bitsail.connector.redis.error.RedisPluginErrorCode;
import com.bytedance.bitsail.connector.redis.option.RedisWriterOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

import static com.bytedance.bitsail.connector.redis.constant.RedisConstants.REDIS_CONNECTOR_NAME;

public class RedisSink<CommitT extends Serializable> implements Sink<Row, CommitT, EmptyState> {
  private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);

  private static final long serialVersionUID = -2257717951626656731L;

  private RedisOptions redisOptions;

  private JedisPoolOptions jedisPoolOptions;

  /**
   * Complex type command with ttl.
   */
  private boolean complexTypeWithTtl;

  /**
   * Command used in the job.
   */
  private JedisCommandDescription commandDescription;

  @Override
  public String getWriterName() {
    return REDIS_CONNECTOR_NAME;
  }

  private void initRedisOptions(BitSailConfiguration writerConfiguration) {
    LOG.info("Start to init RedisOptions!");
    RedisOptions.RedisOptionsBuilder builder = RedisOptions.builder()
        .redisHost(writerConfiguration.getNecessaryOption(RedisWriterOptions.HOST, RedisPluginErrorCode.REQUIRED_VALUE))
        .redisPort(writerConfiguration.getNecessaryOption(RedisWriterOptions.PORT, RedisPluginErrorCode.REQUIRED_VALUE))
        .redisPassword(writerConfiguration.get(RedisWriterOptions.PASSWORD))
        .timeout(writerConfiguration.get(RedisWriterOptions.CLIENT_TIMEOUT_MS))
        .batchInterval(writerConfiguration.get(RedisWriterOptions.WRITE_BATCH_INTERVAL))
        .logSampleInterval(writerConfiguration.get(RedisWriterOptions.LOG_SAMPLE_INTERVAL))
        .maxAttemptCount(writerConfiguration.get(RedisWriterOptions.MAX_ATTEMPT_COUNT));

    // initialize ttl
    int ttl = writerConfiguration.getUnNecessaryOption(RedisWriterOptions.TTL, -1);
    TtlType ttlType;
    try {
      ttlType = TtlType.valueOf(StringUtils.upperCase(writerConfiguration.get(RedisWriterOptions.TTL_TYPE)));
    } catch (IllegalArgumentException e) {
      throw BitSailException.asBitSailException(RedisPluginErrorCode.ILLEGAL_VALUE,
          String.format("unknown ttl type: %s", writerConfiguration.get(RedisWriterOptions.TTL_TYPE)));
    }
    int ttlInSeconds = getTtlInSeconds(ttl, ttlType);
    builder.ttlInSeconds(ttlInSeconds);
    LOG.info("ttl is {}(s)", ttlInSeconds);

    // initialize commandDescription
    String redisDataType = StringUtils.upperCase(writerConfiguration.get(RedisWriterOptions.REDIS_DATA_TYPE));
    String additionalKey = writerConfiguration.getUnNecessaryOption(RedisWriterOptions.ADDITIONAL_KEY, "default_redis_key");
    commandDescription = initJedisCommandDescription(redisDataType, ttlInSeconds, additionalKey);
    int columnSize = commandDescription.getJedisCommand().getColumnSize();
    List<ColumnInfo> columnInfos = writerConfiguration.get(RedisWriterOptions.COLUMNS);
    RowTypeInfo rowTypeInfo = getRowTypeInfo(columnInfos);
    LOG.info("Output Row Type Info: " + rowTypeInfo);
    redisOptions = builder.commandDescription(commandDescription)
        .columnSize(columnSize)
        .rowTypeInfo(rowTypeInfo)
        .complexTypeWithTtl(complexTypeWithTtl)
        .build();
  }

  private void initJedisPoolOptions(BitSailConfiguration writerConfiguration) {
    LOG.info("Start to init JedisPoolOptions!");
    jedisPoolOptions = JedisPoolOptions.builder()
        .maxTotalConnection(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_TOTAL_CONNECTIONS))
        .maxIdleConnection(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_IDLE_CONNECTIONS))
        .minIdleConnection(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MIN_IDLE_CONNECTIONS))
        .maxWaitTimeInMillis(writerConfiguration.get(RedisWriterOptions.JEDIS_POOL_MAX_WAIT_TIME_IN_MILLIS))
        .build();
  }

  @Override
  public void configure(BitSailConfiguration commonConfiguration, BitSailConfiguration writerConfiguration) {
    initRedisOptions(writerConfiguration);
    initJedisPoolOptions(writerConfiguration);
  }

  @Override
  public Writer<Row, CommitT, EmptyState> createWriter(Writer.Context<EmptyState> context) {
    return new RedisWriter<>(redisOptions, jedisPoolOptions);
  }

  @Override
  public TypeInfoConverter createTypeInfoConverter() {
    return new BitSailTypeInfoConverter();
  }

  @Override
  public Optional<WriterCommitter<CommitT>> createCommitter() {
    return Sink.super.createCommitter();
  }

  @Override
  public BinarySerializer<CommitT> getCommittableSerializer() {
    return Sink.super.getCommittableSerializer();
  }

  private int getTtlInSeconds(int ttl, TtlType ttlType) {
    if (ttl < 0) {
      return -1;
    }
    return ttl * ttlType.getContainSeconds();
  }

  private RowTypeInfo getRowTypeInfo(List<ColumnInfo> columns) {
    return commandDescription.getJedisCommand().getRowTypeInfo();
  }

  public JedisCommandDescription initJedisCommandDescription(String redisDataType, int ttlSeconds, String additionalKey) {
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
      case SORTED_SET:
        jedisCommand = JedisCommand.ZADD;
        break;
      default:
        throw BitSailException.asBitSailException(CommonErrorCode.CONFIG_ERROR, "The configure date type " + redisDataType +
            " is not supported, only support string, set, hash, sorted set.");
    }
    if (ttlSeconds <= 0) {
      return new JedisCommandDescription(jedisCommand, additionalKey);
    }
    return new JedisCommandDescription(jedisCommand, additionalKey, ttlSeconds);
  }
}

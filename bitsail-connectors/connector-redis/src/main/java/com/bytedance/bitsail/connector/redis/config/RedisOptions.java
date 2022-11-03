package com.bytedance.bitsail.connector.redis.config;

import com.bytedance.bitsail.connector.redis.core.jedis.JedisCommandDescription;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.Serializable;
@Builder
@AllArgsConstructor
@Data
public class RedisOptions implements Serializable {
  private static final long serialVersionUID = 1865789744864217944L;

  private String redisHost;
  private int redisPort;
  private String redisPassword;
  private int timeout;
  /**
   * Expiring times in seconds.
   */
  private int ttlInSeconds;

  /**
   * Batch send by pipeline after 'batchInterval' records.
   */
  private int batchInterval;

  /**
   * Command used in the job.
   */
  private JedisCommandDescription commandDescription;

  /**
   * Number of columns to send in each record.
   */
  private int columnSize;
  private RowTypeInfo rowTypeInfo;

  /**
   * Log interval of pipelines.
   */
  private int logSampleInterval;

  /**
   * Complex type command with ttl.
   */
  private boolean complexTypeWithTtl;
}

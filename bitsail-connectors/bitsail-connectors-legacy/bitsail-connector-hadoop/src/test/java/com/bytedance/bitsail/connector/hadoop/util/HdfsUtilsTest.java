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

package com.bytedance.bitsail.connector.hadoop.util;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.AttemptTimeLimiters;
import com.github.rholder.retry.RetryListener;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class HdfsUtilsTest {

  @Test
  public void testTimeoutFuseRetryer() {
    AtomicInteger prevInt = new AtomicInteger(1);
    AtomicInteger afterInt = new AtomicInteger(1);
    Callable<Object> call = () -> {
      prevInt.incrementAndGet();
      Thread.sleep(3000);
      afterInt.incrementAndGet();
      return null;
    };

    Retryer<Object> timeoutFuseRetryer = RetryerBuilder.newBuilder()
        .retryIfException()
        .withRetryListener(new RetryListener() {
          @Override
          public <V> void onRetry(Attempt<V> attempt) {
            if (attempt.hasException()) {
              log.error("Retry hdfs operation failed.", attempt.getExceptionCause());
            }
          }
        })
        .withAttemptTimeLimiter(AttemptTimeLimiters.fixedTimeLimit(100, TimeUnit.MILLISECONDS))
        .withWaitStrategy(WaitStrategies.fixedWait(100, TimeUnit.MILLISECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(1))
        .build();

    try {
      HdfsUtils.hdfsRetry(timeoutFuseRetryer, call);
    } catch (Exception e) {
      //just ignore
    }

    Assert.assertEquals(2, prevInt.get());
    Assert.assertEquals(1, afterInt.get());
  }
}

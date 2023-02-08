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

package com.bytedance.bitsail.common.util.timelimit;

import com.github.rholder.retry.AttemptTimeLimiter;
import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class FixedAttemptTimeLimit<V> implements AttemptTimeLimiter<V> {

  private final TimeLimiter timeLimiter;
  private final long duration;
  private final TimeUnit timeUnit;

  public FixedAttemptTimeLimit(long duration, @Nonnull TimeUnit timeUnit) {
    this(new SimpleTimeLimiter(), duration, timeUnit);
  }

  public FixedAttemptTimeLimit(long duration, @Nonnull TimeUnit timeUnit, @Nonnull ExecutorService executorService) {
    this(new SimpleTimeLimiter(executorService), duration, timeUnit);
  }

  private FixedAttemptTimeLimit(@Nonnull TimeLimiter timeLimiter, long duration, @Nonnull TimeUnit timeUnit) {
    Preconditions.checkNotNull(timeLimiter);
    Preconditions.checkNotNull(timeUnit);
    this.timeLimiter = timeLimiter;
    this.duration = duration;
    this.timeUnit = timeUnit;
  }

  @Override
  public V call(Callable<V> callable) throws Exception {
    return timeLimiter.callWithTimeout(callable, duration, timeUnit, true);
  }
}

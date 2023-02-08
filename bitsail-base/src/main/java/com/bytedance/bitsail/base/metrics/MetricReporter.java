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

package com.bytedance.bitsail.base.metrics;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import com.codahale.metrics.Metric;

import java.io.Closeable;

/**
 * Reporter is used to export metric to external backend.
 */
public interface MetricReporter extends Closeable {

  /**
   * Config the metric reporter
   *
   * @param configuration configuration
   */
  void open(BitSailConfiguration configuration);

  /**
   * Close reporter and release resources
   */
  @Override
  void close();

  /**
   * Report the current measurements.
   */
  void report();

  // ------------------------------------------------------------------------
  //  adding / removing metrics
  // ------------------------------------------------------------------------

  /**
   * Called when a new {@link Metric} was added.
   *
   * @param metric     the metric that was added
   * @param metricName the name of the metric
   * @param group      the group that contains the metric
   */
  void notifyOfAddedMetric(Metric metric, String metricName, MetricManager group);

  /**
   * Called when a {@link Metric} was removed.
   *
   * @param metric     the metric that should be removed
   * @param metricName the name of the metric
   * @param group      the group that contains the metric
   */
  void notifyOfRemovedMetric(Metric metric, String metricName, MetricManager group);
}

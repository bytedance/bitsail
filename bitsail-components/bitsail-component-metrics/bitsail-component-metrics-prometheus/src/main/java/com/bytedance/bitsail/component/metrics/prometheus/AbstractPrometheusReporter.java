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

package com.bytedance.bitsail.component.metrics.prometheus;

import com.bytedance.bitsail.base.metrics.MetricManager;
import com.bytedance.bitsail.base.metrics.MetricReporter;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.prometheus.client.CollectorRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AbstractPrometheusReporter implements MetricReporter {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractPrometheusReporter.class);
  private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");

  // A separate registry is used, as the default registry may contain other metrics such as those from the Process Collector.
  public final CollectorRegistry defaultRegistry = new CollectorRegistry(true);
  public final MetricRegistry metricRegistry = new MetricRegistry();

  @Override
  public void notifyOfAddedMetric(Metric metric, String metricName, MetricManager group) {
    String validMetricName = validateMetricName(metricName);
    metricRegistry.register(validMetricName, metric);
  }

  @Override
  public void notifyOfRemovedMetric(Metric metric, String metricName, MetricManager group) {
    String validMetricName = validateMetricName(metricName);
    metricRegistry.remove(validMetricName);
  }

  @Override
  public void close() {
    defaultRegistry.clear();
  }

  @VisibleForTesting
  public String validateMetricName(final String metricName) {
    // refer to https://prometheus.io/docs/instrumenting/writing_exporters/
    // Only [a-zA-Z0-9:_] are valid in prometheus metric names,
    // any other characters should be sanitized to an underscore.
    Matcher matcher = UNALLOWED_CHAR_PATTERN.matcher(metricName);
    if (matcher.find()) {
      String validMetricName = matcher.replaceAll("_");
      LOG.warn("Only [a-zA-Z0-9:_] are valid in prometheus metric names, " +
          "any other characters should be sanitized to an underscore. " +
              "The original name {} will be replaced by {}.", metricName, validMetricName);
      return validMetricName;
    }
    return metricName;
  }
}

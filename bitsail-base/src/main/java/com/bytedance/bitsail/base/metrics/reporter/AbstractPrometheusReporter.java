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

package com.bytedance.bitsail.base.metrics.reporter;

import com.bytedance.bitsail.base.metrics.MetricManager;
import com.bytedance.bitsail.base.metrics.MetricReporter;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

@Slf4j
public abstract class AbstractPrometheusReporter implements MetricReporter {
  private static final Pattern UNALLOWED_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9:_]");

  public final MetricRegistry metricRegistry = new MetricRegistry();

  @Override
  public void notifyOfAddedMetric(Metric metric, String metricName, MetricManager group) {
    String metricInfo = replaceInvalidChars(metricName);
    metricRegistry.register(metricInfo, metric);
  }

  @Override
  public void notifyOfRemovedMetric(Metric metric, String metricName, MetricManager group) {
    String metricInfo = replaceInvalidChars(metricName);
    metricRegistry.remove(metricInfo);
  }

  @Override
  public void close() {
    CollectorRegistry.defaultRegistry.clear();
  }

  String replaceInvalidChars(final String input) {
    // https://prometheus.io/docs/instrumenting/writing_exporters/
    // Only [a-zA-Z0-9:_] are valid in metric names, any other characters should be sanitized to
    // an underscore.
    return UNALLOWED_CHAR_PATTERN.matcher(input).replaceAll("_");
  }
}

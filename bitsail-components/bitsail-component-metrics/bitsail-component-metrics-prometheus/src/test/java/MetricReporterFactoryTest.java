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

import com.bytedance.bitsail.base.metrics.MetricReporter;
import com.bytedance.bitsail.base.metrics.reporter.MetricReporterFactory;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.component.metrics.prometheus.impl.PrometheusMetricReporter;
import com.bytedance.bitsail.component.metrics.prometheus.impl.PrometheusPushGatewayReporter;

import org.junit.Assert;
import org.junit.Test;

public class MetricReporterFactoryTest {

  @Test
  public void getPrometheusMetricReporterTest() {
    String metricReporterType = "prometheus";
    BitSailConfiguration bitSailConfiguration = BitSailConfiguration.newDefault();
    bitSailConfiguration.set(CommonOptions.METRICS_REPORTER_TYPE, metricReporterType);
    MetricReporter metricReporter = MetricReporterFactory.getMetricReporter(bitSailConfiguration);

    Assert.assertEquals(metricReporter.getClass(), PrometheusMetricReporter.class);
  }

  @Test
  public void getPrometheusPushGatewayReporterTest() {
    String metricReporterType = "prometheus_pushgateway";
    BitSailConfiguration bitSailConfiguration = BitSailConfiguration.newDefault();
    bitSailConfiguration.set(CommonOptions.METRICS_REPORTER_TYPE, metricReporterType);
    MetricReporter metricReporter = MetricReporterFactory.getMetricReporter(bitSailConfiguration);

    Assert.assertEquals(metricReporter.getClass(), PrometheusPushGatewayReporter.class);
  }
}

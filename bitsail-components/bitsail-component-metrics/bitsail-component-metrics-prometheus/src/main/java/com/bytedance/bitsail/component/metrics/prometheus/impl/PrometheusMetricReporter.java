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

package com.bytedance.bitsail.component.metrics.prometheus.impl;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.component.metrics.prometheus.AbstractPrometheusReporter;
import com.bytedance.bitsail.component.metrics.prometheus.option.PrometheusOptions;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.HTTPServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;

@Slf4j
public class PrometheusMetricReporter extends AbstractPrometheusReporter {
  private HTTPServer httpServer;
  private DropwizardExports metricExports;
  @Override
  public void open(BitSailConfiguration configuration) {
    int serverPort = configuration.get(PrometheusOptions.PROMETHEUS_PORT_NUM);
    metricExports = new DropwizardExports(metricRegistry);
    metricExports.register(CollectorRegistry.defaultRegistry);
    try {
      this.httpServer = new HTTPServer(new InetSocketAddress(serverPort), CollectorRegistry.defaultRegistry);
      log.info("Started PrometheusReporter HTTP server on port {}.", serverPort);
    } catch (IOException ioe) {
      // assume port conflict
      log.debug("Could not start PrometheusReporter HTTP server on port {}.", serverPort, ioe);
    }
  }

  @Override
  public void close() {
    if (this.httpServer != null) {
      this.httpServer.stop();
    }
    super.close();
  }

  @Override
  public void report() {}
}

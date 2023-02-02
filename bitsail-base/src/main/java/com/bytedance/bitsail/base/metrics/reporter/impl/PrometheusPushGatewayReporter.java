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

package com.bytedance.bitsail.base.metrics.reporter.impl;

import com.bytedance.bitsail.base.metrics.Scheduled;
import com.bytedance.bitsail.base.metrics.reporter.AbstractPrometheusReporter;
import com.bytedance.bitsail.base.metrics.reporter.impl.option.PrometheusPushGatewayOptions;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;

@Slf4j
public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {
  private int periodSeconds;
  private PushGateway pushGateway;
  private String jobName;
  private boolean deleteOnShutdown;
  DropwizardExports dropwizardExports;

  @Override
  public void open(BitSailConfiguration configuration) {
    String serverHost = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_HOST_NAME);
    int serverPort = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_PORT_NUM);
    periodSeconds = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS);
    jobName = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_JOBNAME);
    deleteOnShutdown = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE);
    pushGateway = createPushGatewayClient(serverHost, serverPort);
    dropwizardExports = new DropwizardExports(metricRegistry);
    dropwizardExports.register(CollectorRegistry.defaultRegistry);
  }

  @SuppressWarnings("checkstyle:MagicNumber")
  private PushGateway createPushGatewayClient(String serverHost, int serverPort) {
    if (serverPort == 443) {
      try {
        return new PushGateway(new URL("https://" + serverHost + ":" + serverPort));
      } catch (MalformedURLException e) {
        e.printStackTrace();
        throw new IllegalArgumentException("Malformed pushgateway host: " + serverHost);
      }
    }
    return new PushGateway(serverHost + ":" + serverPort);
  }

  @Override
  public void close() {
    if (deleteOnShutdown && pushGateway != null) {
      try {
        pushGateway.delete(jobName);
      } catch (IOException e) {
        log.warn(
            "Failed to delete metrics from PushGateway with jobName {}.",
            jobName,
            e);
      }
    }
    super.close();
  }

  @Override
  public void report() {
    try {
      pushGateway.pushAdd(CollectorRegistry.defaultRegistry, jobName);
    } catch (Exception e) {
      log.warn(
          "Failed to push metrics to PushGateway with jobName {}.",
          jobName,
          e);
    }
  }

  @Override
  public long getDelay() {
    return periodSeconds;
  }

  @Override
  public TimeUnit getDelayTimeUnit() {
    return TimeUnit.SECONDS;
  }
}

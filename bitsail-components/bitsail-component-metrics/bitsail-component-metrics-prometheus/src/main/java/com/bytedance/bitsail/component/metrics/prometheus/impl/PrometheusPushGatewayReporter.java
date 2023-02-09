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

package com.bytedance.bitsail.component.metrics.prometheus.impl;

import com.bytedance.bitsail.base.metrics.Scheduled;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.component.metrics.prometheus.AbstractPrometheusReporter;
import com.bytedance.bitsail.component.metrics.prometheus.error.PrometheusPushGatewayErrorCode;
import com.bytedance.bitsail.component.metrics.prometheus.option.PrometheusPushGatewayOptions;

import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.PushGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class PrometheusPushGatewayReporter extends AbstractPrometheusReporter implements Scheduled {
  private static final Logger LOG = LoggerFactory.getLogger(PrometheusPushGatewayReporter.class);
  private int periodSeconds;
  private PushGateway pushGateway;
  private String jobName;
  private boolean deleteOnShutdown;
  private DropwizardExports dropwizardExports;
  private Map<String, String> groupingKey;
  private int httpsPort;

  @Override
  public void open(BitSailConfiguration configuration) {
    String serverHost = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_HOST_NAME);
    int serverPort = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_PORT);
    periodSeconds = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_REPORT_PERIOD_IN_SECONDS);

    deleteOnShutdown = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_DELETE_ON_SHUTDOWN_ENABLE);
    if (configuration.fieldExists(PrometheusPushGatewayOptions.PUSHGATEWAY_JOBNAME)) {
      jobName = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_JOBNAME);
    } else {
      jobName = configuration.get(CommonOptions.JOB_NAME) +
          configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_DEFAULT_JOBNAME_SUFFIX);
    }
    httpsPort = configuration.get(PrometheusPushGatewayOptions.PUSHGATEWAY_HTTPS_PORT);
    pushGateway = createPushGatewayClient(serverHost, serverPort);
    dropwizardExports = new DropwizardExports(metricRegistry);
    dropwizardExports.register(defaultRegistry);

    groupingKey = new HashMap<>();
    groupingKey.put("instance", String.valueOf(configuration.get(CommonOptions.INSTANCE_ID)));
  }

  private PushGateway createPushGatewayClient(String serverHost, int serverPort) {
    if (serverPort == httpsPort) {
      try {
        return new PushGateway(new URL("https://" + serverHost + ":" + serverPort));
      } catch (MalformedURLException e) {
        throw BitSailException.asBitSailException(
            PrometheusPushGatewayErrorCode.CLIENT_ERROR,
            "Malformed pushgateway host: " + serverHost + ":" + serverPort,
            e);
      }
    }
    return new PushGateway(serverHost + ":" + serverPort);
  }

  @Override
  public void close() {
    if (deleteOnShutdown && pushGateway != null) {
      try {
        pushGateway.delete(jobName, groupingKey);
      } catch (IOException e) {
        LOG.warn(
            "Failed to delete metrics from PushGateway with jobName {}, groupingKey {}.",
            jobName,
            groupingKey,
            e);
      }
    }
    super.close();
  }

  @Override
  public void report() {
    try {
      pushGateway.pushAdd(defaultRegistry, jobName, groupingKey);
    } catch (Exception e) {
      LOG.warn(
          "Failed to push metrics to PushGateway with jobName {}, groupingKey {}.",
          jobName,
          groupingKey,
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

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

package com.bytedance.bitsail.test.e2e;

import com.bytedance.bitsail.base.packages.LocalFSPluginFinder;
import com.bytedance.bitsail.base.packages.PluginFinder;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.datasource.AbstractDataSource;
import com.bytedance.bitsail.test.e2e.datasource.EmptyDataSource;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;
import com.bytedance.bitsail.test.e2e.executor.flink.AbstractFlinkExecutor;
import com.bytedance.bitsail.test.e2e.executor.flink.Flink11Executor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
@Builder
public class TestJob implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TestJob.class);

  public static final int SUCCESS_EXIT_CODE = 0;
  public static final int FAILURE_EXIT_CODE = 1;
  public static final int VALIDATION_EXIT_CODE = 2;

  /**
   * Format the test case name, test_{sourceType}_to_{sinkType}_on_{engine_type}.
   */
  protected static final String TEST_CASE_NAME_TEMPLATE = "test_%s_to_%s_on_%s";

  /**
   * Job conf to run.
   */
  protected final BitSailConfiguration jobConf;

  /**
   * Engine type.
   */
  protected final String engineType;

  /**
   * Data source type for reader.
   */
  protected final String sourceType;

  /**
   * Data source type for writer.
   */
  protected final String sinkType;

  /**
   * Plugin finder for loading datasource and engine libs.
   */
  protected PluginFinder pluginFinder;

  /**
   * Data source for reader.
   */
  protected AbstractDataSource source;

  /**
   * Data source for writer.
   */
  protected AbstractDataSource sink;

  /**
   * Executor.
   */
  protected AbstractExecutor executor;

  TestJob(BitSailConfiguration jobConf, String sourceType, String sinkType, String engineType) {
    this.jobConf = jobConf;
    this.sourceType = sourceType.trim().toLowerCase();
    this.sinkType = sinkType.trim().toLowerCase();
    this.engineType = engineType.trim().toLowerCase();
  }

  /**
   * Prepare data sources and executor.
   */
  protected void init() {
    pluginFinder = new LocalFSPluginFinder();
    pluginFinder.configure(jobConf);
    source = prepareSource(jobConf);
    sink = prepareSink(jobConf);
    executor = prepareExecutor(jobConf);
  }

  /**
   * Create data source for reader.
   */
  protected AbstractDataSource prepareSource(BitSailConfiguration jobConf) {
    AbstractDataSource dataSource;
    if ("empty".equals(sourceType)) {
      dataSource = new EmptyDataSource();
    } else {
      try {
        dataSource = pluginFinder.findPluginInstance(sourceType);
      } catch (BitSailException e) {
        dataSource = new EmptyDataSource();
      }
    }
    dataSource.configure(jobConf);
    dataSource.start();
    dataSource.fillData();

    LOG.info("DataSource {} is started as source in [{}].", sourceType, dataSource.getContainerName());
    return dataSource;
  };

  /**
   * Create data source for writer.
   */
  protected AbstractDataSource prepareSink(BitSailConfiguration jobConf) {
    AbstractDataSource dataSource;
    if ("empty".equals(sinkType)) {
      dataSource = new EmptyDataSource();
    } else {
      try {
        dataSource = pluginFinder.findPluginInstance(sinkType);
      } catch (BitSailException e) {
        dataSource = new EmptyDataSource();
      }
    }
    dataSource.configure(jobConf);
    dataSource.start();

    LOG.info("DataSource {} is started as sink in [{}].", sourceType, dataSource.getContainerName());
    return dataSource;
  }

  /**
   * Create test executor.
   */
  protected AbstractExecutor prepareExecutor(BitSailConfiguration jobConf) {
    switch (engineType) {
      case "flink11":
        executor = new Flink11Executor();
        ((AbstractFlinkExecutor) executor).setPluginFinder(pluginFinder);
        break;
      default:
        throw new UnsupportedOperationException("engine type " + engineType + " is not supported yet.");
    }

    executor.configure(jobConf);
    executor.init();
    return executor;
  }

  /**
   * Run a job.
   */
  public int run() throws Exception {
    init();

    int exitCode;
    try {
      exitCode = executor.run(String.format(TEST_CASE_NAME_TEMPLATE, sourceType, sinkType, engineType));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    if (exitCode != 0) {
      LOG.error("Test return code is {}, will directly exit.", exitCode);
      return exitCode;
    }

    try {
      sink.validate();
    } catch (BitSailException ex) {
      LOG.error("Failed to validate sink data after job.", ex);
      return VALIDATION_EXIT_CODE;
    }

    return exitCode;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(executor);
    IOUtils.closeQuietly(source);
    IOUtils.closeQuietly(sink);
  }

  /**
   * @return A Job Builder.
   */
  public static TestJobBuilder builder() {
    return new TestJobBuilder();
  }

  /**
   * Job Builder.
   */
  public static class TestJobBuilder {
    private BitSailConfiguration jobConf;
    private String sourceType;
    private String sinkType;
    private String engineType;

    public TestJobBuilder withJobConf(BitSailConfiguration jobConf) {
      this.jobConf = jobConf;
      return this;
    }

    public TestJobBuilder withSourceType(String sourceType) {
      this.sourceType = sourceType;
      return this;
    }

    public TestJobBuilder withSinkType(String sinkType) {
      this.sinkType = sinkType;
      return this;
    }

    public TestJobBuilder withEngineType(String engineType) {
      this.engineType = engineType;
      return this;
    }

    public TestJob build() {
      Preconditions.checkNotNull(jobConf);
      Preconditions.checkNotNull(sourceType);
      Preconditions.checkNotNull(sinkType);
      Preconditions.checkNotNull(engineType);
      return new TestJob(jobConf, sourceType, sinkType, engineType);
    }
  }
}

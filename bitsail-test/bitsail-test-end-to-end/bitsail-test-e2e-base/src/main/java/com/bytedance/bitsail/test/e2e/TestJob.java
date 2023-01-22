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

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.datasource.AbstractDataSource;
import com.bytedance.bitsail.test.e2e.datasource.DataSourceFactory;
import com.bytedance.bitsail.test.e2e.datasource.EmptyDataSource;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;
import com.bytedance.bitsail.test.e2e.executor.flink.Flink11Executor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.util.function.Consumer;

@AllArgsConstructor
@Builder
public class TestJob implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(TestJob.class);

  public static final int SUCCESS_EXIT_CODE = 0;
  public static final int FAILURE_EXIT_CODE = 1;
  public static final int VALIDATION_EXIT_CODE = 2;

  /**
   * Job conf to run.
   */
  protected final BitSailConfiguration jobConf;

  /**
   * Engine type.
   */
  protected final String engineType;

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

  TestJob(BitSailConfiguration jobConf, String engineType) {
    this.jobConf = jobConf;
    this.engineType = engineType.trim().toLowerCase();
  }

  /**
   * Prepare data sources and executor.
   */
  protected void init() {
    executor = createExecutor();
    Network executorNetwork = executor.getNetwork();

    source = prepareSource(jobConf, executorNetwork);
    sink = prepareSink(jobConf, executorNetwork);
  }

  /**
   * Create data source for reader.
   */
  protected AbstractDataSource prepareSource(BitSailConfiguration jobConf,
                                             Network executorNetwork) {
    AbstractDataSource dataSource;
    try {
      dataSource = DataSourceFactory.getAsSource(jobConf);
    } catch (BitSailException e) {
      dataSource = new EmptyDataSource();
    }
    if (dataSource == null) {
      dataSource = new EmptyDataSource();
    }
    dataSource.configure(jobConf);
    dataSource.initNetwork(executorNetwork);
    dataSource.start();
    dataSource.fillData();

    LOG.info("DataSource is started as source in [{}].", dataSource.getContainerName());
    return dataSource;
  }

  /**
   * Create data source for writer.
   */
  protected AbstractDataSource prepareSink(BitSailConfiguration jobConf,
                                           Network executorNetwork) {
    AbstractDataSource dataSource;
    try {
      dataSource = DataSourceFactory.getAsSink(jobConf);
    } catch (BitSailException e) {
      dataSource = new EmptyDataSource();
    }
    if (dataSource == null) {
      dataSource = new EmptyDataSource();
    }
    dataSource.configure(jobConf);
    dataSource.initNetwork(executorNetwork);
    dataSource.start();

    LOG.info("DataSource is started as sink in [{}].", dataSource.getContainerName());
    return dataSource;
  }

  /**
   * Create test executor.
   */
  protected AbstractExecutor createExecutor() {
    switch (engineType) {
      case "flink11":
        executor = new Flink11Executor();
        break;
      default:
        throw new UnsupportedOperationException("engine type " + engineType + " is not supported yet.");
    }
    return executor;
  }

  /**
   * Run a job.
   */
  public int run(String caseName) throws Exception {
    init();

    source.modifyJobConf(jobConf);
    sink.modifyJobConf(jobConf);

    executor.configure(jobConf);
    executor.init();

    int exitCode;
    try {
      exitCode = executor.run(String.format("%s_on_%s", caseName, engineType));
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    return exitCode;
  }

  /**
   * Validate result in sink after run.
   */
  public void validate(Consumer<AbstractDataSource> validation) {
    if (validation != null) {
      LOG.info("Start validation...");
      try {
        validation.accept(sink);
      } catch (Exception e) {
        throw BitSailException.asBitSailException(CommonErrorCode.VALIDATION_EXCEPTION, e);
      }
      LOG.info("Pass validation!");
    }
  }

  @Override
  public void close() {
    LOG.info("Test Job Closing...");
    executor.closeQuietly();
    source.closeQuietly();
    sink.closeQuietly();
    LOG.info("Test Job Closed!");
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
    private String engineType;

    public TestJobBuilder withJobConf(BitSailConfiguration jobConf) {
      this.jobConf = jobConf;
      return this;
    }

    public TestJobBuilder withEngineType(String engineType) {
      this.engineType = engineType;
      return this;
    }

    public TestJob build() {
      Preconditions.checkNotNull(jobConf);
      Preconditions.checkNotNull(engineType);
      return new TestJob(jobConf, engineType);
    }
  }
}

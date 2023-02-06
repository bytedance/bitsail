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
import com.bytedance.bitsail.test.e2e.option.EndToEndOptions;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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
    AbstractDataSource dataSource = null;
    String dataSourceClass = jobConf.get(EndToEndOptions.E2E_READER_DATA_SOURCE_CLASS);

    if (dataSourceClass != null) {
      try {
        LOG.info("Reader data source class name: [{}]", dataSourceClass);
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(dataSourceClass);
        dataSource = (AbstractDataSource) clazz.newInstance();
      } catch (Exception e) {
        LOG.error("Failed to create data source [{}], will try using class name.", dataSourceClass, e);
        dataSource = null;
      }
    }

    if (dataSource == null) {
      try {
        dataSource = DataSourceFactory.getAsSource(jobConf);
      } catch (BitSailException e) {
        LOG.error("Failed create data source from factory, will use empty source.", e);
        dataSource = new EmptyDataSource();
      }
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
    AbstractDataSource dataSource = null;
    String dataSourceClass = jobConf.get(EndToEndOptions.E2E_WRITER_DATA_SOURCE_CLASS);

    if (dataSourceClass != null) {
      try {
        LOG.info("Writer data source class name: [{}]", dataSourceClass);
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(dataSourceClass);
        dataSource = (AbstractDataSource) clazz.newInstance();
      } catch (Exception e) {
        LOG.error("Failed to create data source [{}], will try using class name.", dataSourceClass, e);
        dataSource = null;
      }
    }

    if (dataSource == null) {
      try {
        dataSource = DataSourceFactory.getAsSink(jobConf);
      } catch (BitSailException e) {
        LOG.error("Failed create data source from factory, will use empty source.", e);
        dataSource = new EmptyDataSource();
      }
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
  public int run(String caseName,
                 int allowedTimeout) throws Exception {
    init();

    source.modifyJobConf(jobConf);
    sink.modifyJobConf(jobConf);

    executor.configure(jobConf);
    executor.init();

    boolean allowTimeout = allowedTimeout > 0;
    int execTimeout = allowTimeout ? allowedTimeout : 300;

    ExecutorService service = Executors.newSingleThreadExecutor();
    try {
      Future<?> future = service.submit(() ->
          executor.run(String.format("%s_on_%s", caseName, engineType)));
      return (Integer) future.get(execTimeout, TimeUnit.SECONDS);
    } catch (TimeoutException te) {
      if (allowTimeout) {
        LOG.info("Execute more than {} seconds, will terminate it.", allowedTimeout);
        return 0;
      } else {
        LOG.error("Execute more than {} seconds.", allowedTimeout);
        throw te;
      }
    } catch (Exception e) {
      LOG.error("Failed to execute job.", e);
      throw e;
    }
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
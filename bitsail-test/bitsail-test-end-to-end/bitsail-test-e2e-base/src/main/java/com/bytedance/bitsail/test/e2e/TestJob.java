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
import com.bytedance.bitsail.test.e2e.base.AbstractContainer;
import com.bytedance.bitsail.test.e2e.datasource.AbstractDataSource;
import com.bytedance.bitsail.test.e2e.datasource.DataSourceFactory;
import com.bytedance.bitsail.test.e2e.datasource.EmptyDataSource;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;
import com.bytedance.bitsail.test.e2e.executor.ExecutorLoader;
import com.bytedance.bitsail.test.e2e.option.EndToEndOptions;

import lombok.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

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
  protected List<AbstractExecutor> executors;

  /**
   * Network for executor and data sources.
   */
  @Builder.Default
  protected Network network = Network.newNetwork();

  /**
   * If to recycle data source (source).
   */
  @Builder.Default
  protected boolean recycleSource = true;

  /**
   * If to reuse data source (sink).
   */
  @Builder.Default
  protected boolean recycleSink = true;

  /**
   * Create data source for reader.
   */
  protected AbstractDataSource prepareSource(BitSailConfiguration jobConf,
                                             Network executorNetwork,
                                             boolean recycle) {
    if (source != null && recycle) {
      source.reset();
      return source;
    }

    String dataSourceClass = jobConf.get(EndToEndOptions.E2E_READER_DATA_SOURCE_CLASS);

    if (dataSourceClass != null) {
      try {
        LOG.info("Reader data source class name: [{}]", dataSourceClass);
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(dataSourceClass);
        source = (AbstractDataSource) clazz.newInstance();
      } catch (Exception e) {
        LOG.error("Failed to create data source [{}], will try using class name.", dataSourceClass, e);
        source = null;
      }
    }

    if (source == null) {
      try {
        source = DataSourceFactory.getAsSource(jobConf);
      } catch (BitSailException e) {
        LOG.error("Failed create data source from factory, will use empty source.", e);
        source = new EmptyDataSource();
      }
    }
    source.configure(jobConf);
    source.initNetwork(executorNetwork);
    source.start();

    LOG.info("DataSource is started as source in [{}].", source.getContainerName());
    return source;
  }

  /**
   * Create data source for writer.
   */
  protected AbstractDataSource prepareSink(BitSailConfiguration jobConf,
                                           Network executorNetwork,
                                           boolean recycle) {
    if (sink != null && recycle) {
      sink.reset();
      return sink;
    }

    String dataSourceClass = jobConf.get(EndToEndOptions.E2E_WRITER_DATA_SOURCE_CLASS);

    if (dataSourceClass != null) {
      try {
        LOG.info("Writer data source class name: [{}]", dataSourceClass);
        Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(dataSourceClass);
        sink = (AbstractDataSource) clazz.newInstance();
      } catch (Exception e) {
        LOG.error("Failed to create data source [{}], will try using class name.", dataSourceClass, e);
        sink = null;
      }
    }

    if (sink == null) {
      try {
        sink = DataSourceFactory.getAsSink(jobConf);
      } catch (BitSailException e) {
        LOG.error("Failed create data source from factory, will use empty source.", e);
        sink = new EmptyDataSource();
      }
    }
    sink.configure(jobConf);
    sink.initNetwork(executorNetwork);
    sink.start();

    LOG.info("DataSource is started as sink in [{}].", sink.getContainerName());
    return sink;
  }

  /**
   * Create test executors.
   */
  protected List<AbstractExecutor> createExecutors() {
    ExecutorLoader loader = new ExecutorLoader(null, null);
    List<AbstractExecutor> executors = loader.loadAll();
    executors.forEach(executor -> executor.initNetwork(network));
    return executors;
  }

  /**
   * Run a job.
   */
  public int run(String caseName, int allowedTimeout) throws Exception {
    executors = createExecutors();

    for (AbstractExecutor executor : executors) {
      source = prepareSource(jobConf, network, recycleSource);
      sink = prepareSink(jobConf, network, recycleSink);

      source.fillData(executor);
      source.modifyJobConf(jobConf);
      sink.modifyJobConf(jobConf);

      executor.configure(jobConf);
      executor.init();

      boolean allowTimeout = allowedTimeout > 0;
      int execTimeout = allowTimeout ? allowedTimeout : 300;

      int exitCode;
      ExecutorService service = Executors.newSingleThreadExecutor();
      try {
        Future<?> future = service.submit(() ->
            executor.run(String.format("%s_on_%s", caseName, executor.getContainerName())));
        exitCode = (Integer) future.get(execTimeout, TimeUnit.SECONDS);
      } catch (TimeoutException te) {
        if (allowTimeout) {
          LOG.info("Execute more than {} seconds, will terminate it.", allowedTimeout);
          exitCode = SUCCESS_EXIT_CODE;
        } else {
          LOG.error("Execute more than {} seconds.", allowedTimeout);
          throw te;
        }
      } catch (Exception e) {
        LOG.error("Failed to execute job.", e);
        throw e;
      }

      LOG.info("Exit code on executor {}: {}", executor.getContainerName(), exitCode);
      if (exitCode != SUCCESS_EXIT_CODE) {
        return exitCode;
      }

      if (!recycleSource) {
        source.closeQuietly();
      }
      if (!recycleSink) {
        sink.closeQuietly();
      }
    }

    return SUCCESS_EXIT_CODE;
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
    if (executors != null) {
      executors.forEach(AbstractContainer::closeQuietly);
    }
    if (source != null) {
      source.closeQuietly();
    }
    if (sink != null) {
      sink.closeQuietly();
    }
    LOG.info("Test Job Closed!");
  }

  /**
   * @return A Job Builder.
   */
  public static TestJobBuilder builder() {
    return new TestJobBuilder();
  }
}

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

package com.bytedance.bitsail.connector.elasticsearch.source.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceEvent;
import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchReaderOptions;
import com.bytedance.bitsail.connector.elasticsearch.source.split.ElasticsearchSourceSplit;
import com.bytedance.bitsail.connector.elasticsearch.utils.SourceSetupUtils;
import com.bytedance.bitsail.test.connector.test.testcontainers.elasticsearch.ElasticsearchCluster;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;

public class ElasticsearchSourceSplitCoordinatorITCase {

  private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchSourceSplitCoordinatorITCase.class);

  private ElasticsearchCluster esCluster;

  private SourceSetupUtils sourceEnv;

  private BitSailConfiguration jobConf;

  @Before
  public void setup() throws IOException, InterruptedException {
    esCluster = new ElasticsearchCluster();
    esCluster.startService();
    esCluster.checkClusterHealth();

    jobConf = BitSailConfiguration.newDefault();
    jobConf.set(ElasticsearchReaderOptions.ES_HOSTS,
        Collections.singletonList(esCluster.getHttpHostAddress()));
    jobConf.set(ElasticsearchReaderOptions.ES_INDEX, "test1, test2, test3");
    jobConf.set(ElasticsearchReaderOptions.READER_PARALLELISM_NUM, 2);
    jobConf.set(ElasticsearchReaderOptions.SPLIT_STRATEGY, "round_robin");

    this.sourceEnv =
        SourceSetupUtils.builder()
            .esCluster(esCluster)
            .jobConf(jobConf)
            .indices(Arrays.asList("test1", "test2", "test3"))
            .count(10)
            .build();

    sourceEnv.start();
  }

  @After
  public void tearDown() throws IOException {
    this.sourceEnv.client.close();
    esCluster.close();
  }

  @Test
  public void testConstructSplit() {

    SourceSplitCoordinator.Context<ElasticsearchSourceSplit, EmptyState> context = new SourceSplitCoordinator.Context<ElasticsearchSourceSplit, EmptyState>() {
      @Override
      public boolean isRestored() {
        return false;
      }

      @Override
      public EmptyState getRestoreState() {
        return new EmptyState();
      }

      @Override
      public int totalParallelism() {
        return 2;
      }

      @Override
      public Set<Integer> registeredReaders() {
        return null;
      }

      @Override
      public void assignSplit(int subtaskId, List<ElasticsearchSourceSplit> splits) {
        // pass
      }

      @Override
      public void signalNoMoreSplits(int subtask) {
        // pass
      }

      @Override
      public void sendEventToSourceReader(int subtaskId, SourceEvent event) {
        // pass
      }

      @Override
      public <T> void runAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, int initialDelay, long interval) {
        // pass
      }

      @Override
      public <T> void runAsyncOnce(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        // pass
      }
    };
    ElasticsearchSourceSplitCoordinator elasticsearchSourceSplitCoordinator = new ElasticsearchSourceSplitCoordinator(context, jobConf);
    elasticsearchSourceSplitCoordinator.start();
    Map<Integer, List<ElasticsearchSourceSplit>> splitAssignmentPlan = elasticsearchSourceSplitCoordinator.getSplitAssignmentPlan();
    LOG.info("Get split assignment plan: {}", splitAssignmentPlan);
    assertEquals("Reader parallelism wrong", 2, splitAssignmentPlan.size());
    assertEquals("Reader-1 accept splits wrong", 2, splitAssignmentPlan.get(0).size());
    assertEquals("Reader-2 accept splits wrong", 1, splitAssignmentPlan.get(1).size());
  }
}

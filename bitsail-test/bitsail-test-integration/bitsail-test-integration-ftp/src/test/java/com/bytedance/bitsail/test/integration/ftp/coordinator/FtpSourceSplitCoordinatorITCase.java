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

package com.bytedance.bitsail.test.integration.ftp.coordinator;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplitCoordinator;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.ftp.core.config.FtpConfig;
import com.bytedance.bitsail.connector.ftp.option.FtpReaderOptions;
import com.bytedance.bitsail.connector.ftp.source.split.FtpSourceSplit;
import com.bytedance.bitsail.connector.ftp.source.split.coordinator.FtpSourceSplitCoordinator;
import com.bytedance.bitsail.test.integration.ftp.container.SftpDataSource;
import com.bytedance.bitsail.test.integration.ftp.container.constant.FtpTestConstants;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class FtpSourceSplitCoordinatorITCase {
  private FtpSourceSplitCoordinator ftpSourceSplitCoordinator;
  private static GenericContainer sftpServer;

  @Before
  public void setup() {
    sftpServer = SftpDataSource.create();
    sftpServer.start();
  }

  @After
  public void teardown() {
    sftpServer.stop();
  }

  @Test
  public void testConstructSplit() {
    BitSailConfiguration jobConf = BitSailConfiguration.newDefault();
    jobConf.set(FtpReaderOptions.PROTOCOL, FtpConfig.Protocol.SFTP.name());
    jobConf.set(FtpReaderOptions.PORT, sftpServer.getFirstMappedPort());
    jobConf.set(FtpReaderOptions.PATH_LIST, "/data/files/p1,/data/files/p2");
    jobConf.set(FtpReaderOptions.ENABLE_SUCCESS_FILE_CHECK, false);
    jobConf.set(FtpReaderOptions.HOST, FtpTestConstants.LOCALHOST);
    jobConf.set(FtpReaderOptions.PASSWORD, FtpTestConstants.PASSWORD);
    jobConf.set(FtpReaderOptions.USER, FtpTestConstants.USER);
    jobConf.set(FtpReaderOptions.TIME_OUT, FtpTestConstants.DEFAULT_TIMEOUT);
    jobConf.set(FtpReaderOptions.CONTENT_TYPE, "csv");
    SourceSplitCoordinator.Context<FtpSourceSplit, EmptyState> context = new SourceSplitCoordinator.Context<FtpSourceSplit, EmptyState>() {

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
      public void assignSplit(int subtaskId, List<FtpSourceSplit> splits) {
        //pass
      }

      @Override
      public void signalNoMoreSplits(int subtask) {
        //pass
      }

      @Override
      public void sendEventToSourceReader(int subtaskId, com.bytedance.bitsail.base.connector.reader.v1.SourceEvent event) {
        //pass
      }

      @Override
      public <T> void runAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, int initialDelay, long interval) {
        //pass
      }

      @Override
      public <T> void runAsyncOnce(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        //pass
      }
    };
    ftpSourceSplitCoordinator = new FtpSourceSplitCoordinator(context, jobConf);
    ftpSourceSplitCoordinator.start();
    List<FtpSourceSplit> splits = ftpSourceSplitCoordinator.getSplitList();
    List<String> actual = splits.stream().map(FtpSourceSplit::getPath).collect(Collectors.toList());
    List<String> expected = Arrays.asList("/data/files/p1/p11/f11", "/data/files/p1/f1", "/data/files/p2/f2");
    Assert.assertTrue(actual.containsAll(expected) && expected.containsAll(actual));
  }

  @Test
  public void testReaderSelector() {
    int readerNum = 4;
    int splitNum = 10;
    Map<Integer, HashSet> result = new HashMap<>();
    for (int i = 0; i < splitNum; i++) {
      int readerIndex = FtpSourceSplitCoordinator.getReaderIndexForTest(readerNum);
      result.computeIfAbsent(readerIndex, k -> new HashSet<>()).add(i);
    }
    int actualNum = 0;
    for (HashSet set : result.values()) {
      Assert.assertTrue(set.size() == 3 || set.size() == 2);
      actualNum += set.size();
    }
    Assert.assertEquals(actualNum, splitNum);
  }
}

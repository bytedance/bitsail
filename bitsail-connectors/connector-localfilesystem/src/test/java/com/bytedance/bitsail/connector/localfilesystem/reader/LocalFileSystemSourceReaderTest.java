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

package com.bytedance.bitsail.connector.localfilesystem.reader;

import com.bytedance.bitsail.base.connector.reader.v1.SourceReader;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfos;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LocalFileSystemSourceReaderTest {
  private void testUnifiedLocalFSSourceReader(String jobConfPath) throws Exception, Error {
    // Using CountDownLatch to mock multi-thread concurrency environment
    SimpleSourcePipeLine<Row> sourcePipeline = new SimpleSourcePipeLine<>();
    BitSailConfiguration jobConf = BitSailConfiguration.from(new File(
        Paths.get(getClass().getClassLoader().getResource(jobConfPath).toURI()).toString()));
    SourceReader.Context context = new SourceReader.Context() {
      @Override
      public RowTypeInfo getRowTypeInfo() {
        return new RowTypeInfo(new String[] {
            "id",
            "date",
            "localdatetime_value",
            "last_name",
            "bool_value"
        }, new TypeInfo[] {
            TypeInfos.LONG_TYPE_INFO,
            TypeInfos.LOCAL_DATE_TYPE_INFO,
            TypeInfos.SQL_TIMESTAMP_TYPE_INFO,
            TypeInfos.STRING_TYPE_INFO,
            TypeInfos.BOOLEAN_TYPE_INFO
        });
      }

      @Override
      public int getIndexOfSubtask() {
        return 0;
      }

      @Override
      public void sendSplitRequest() {

      }
    };
    // control multi sub threads' state
    CountDownLatch begin;
    // control main sub threads' state
    CountDownLatch end;
    ExecutorService cachedThreadPool;
    try (LocalFileSystemSourceReader localFileSystemSourceReader = new LocalFileSystemSourceReader(
        jobConf,
        context
    )) {
      final int N = 10;
      begin = new CountDownLatch(1);
      end = new CountDownLatch(N);
      // create thread pool
      cachedThreadPool = Executors.newCachedThreadPool();

      for (int i = 0; i < N; i++) {
        cachedThreadPool.execute(() -> {
          try {
            begin.await();
            while (localFileSystemSourceReader.hasMoreElements()) {
              localFileSystemSourceReader.pollNext(sourcePipeline);
            }
          } catch (Exception e) {
            throw new RuntimeException(e);
          } finally {
            end.countDown();
          }
        });
      }
      begin.countDown();
    }

    try {
      end.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } finally {
      cachedThreadPool.shutdown();
    }
    // test amount of lines
    Assert.assertEquals(sourcePipeline.getLst().size(), 12);
    // test whether exist duplicated row
    List<Long> idList = sourcePipeline.getLst().stream().map(x -> x.getLong(0)).collect(Collectors.toList());
    Assert.assertEquals(idList.stream().sorted().collect(Collectors.toList()),
        Stream.iterate(0L, item -> item + 1L).limit(12).collect(Collectors.toList()));
  }

  @Test
  public void testUnifiedLocalFSSourceReaderWithCSV() throws Exception {
    final String jobConfPath = "scripts/local-csv-to-print.json";
    new LocalFileSystemSourceReaderTest().testUnifiedLocalFSSourceReader(jobConfPath);
  }

  @Test
  public void testUnifiedLocalFSSourceReaderWithJSON() throws Exception {
    final String jobConfPath = "scripts/local-json-to-print.json";
    new LocalFileSystemSourceReaderTest().testUnifiedLocalFSSourceReader(jobConfPath);
  }
}

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

package com.bytedance.bitsail.connector.legacy.hudi.sink.bulk;

import com.bytedance.bitsail.connector.legacy.hudi.configuration.OptionsResolver;
import com.bytedance.bitsail.connector.legacy.hudi.sink.bucket.BucketBulkInsertWriterHelper;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

/**
 * Factory clazz to generate bulk insert writer helpers.
 */
public class WriterHelpers {
  public static BulkInsertWriterHelper getWriterHelper(Configuration conf, HoodieTable<?, ?, ?, ?> hoodieTable, HoodieWriteConfig writeConfig,
                                                       String instantTime, int taskPartitionId, long taskId, long taskEpochId, RowType rowType) {
    return OptionsResolver.isBucketIndexType(conf) ?
        new BucketBulkInsertWriterHelper(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType)
        : new BulkInsertWriterHelper(conf, hoodieTable, writeConfig, instantTime, taskPartitionId, taskId, taskEpochId, rowType);
  }
}

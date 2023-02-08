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

package com.bytedance.bitsail.connector.legacy.streamingfile.common.filesystem.aggregate;

import com.bytedance.bitsail.connector.legacy.streamingfile.common.filesystem.schema.FileSystemMeta;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Set;

/**
 * Created 2020/11/23.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class GlobalAggregateResult implements Serializable {

  private long minJobTimestamp;

  private FileSystemMeta fileSystemMeta;

  private Set<Tuple2<Long, Long>> pendingCommitPartitionTaskTimes;

  private Set<String> committedPartitions;
}

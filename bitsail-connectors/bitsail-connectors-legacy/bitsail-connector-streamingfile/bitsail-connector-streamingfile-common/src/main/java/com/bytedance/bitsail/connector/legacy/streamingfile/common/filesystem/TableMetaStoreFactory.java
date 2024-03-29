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

package com.bytedance.bitsail.connector.legacy.streamingfile.common.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.Catalog;

import java.io.Closeable;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * Meta store factory to create {@link TableMetaStore}. Meta store may need contains connection
 * to remote, so we should not create too frequently.
 */
@Internal
public interface TableMetaStoreFactory extends Serializable {

  /**
   * Create a {@link TableMetaStore}.
   */
  TableMetaStore createTableMetaStore() throws Exception;

  /**
   * Meta store to manage the location paths of this table and its partitions.
   */
  interface TableMetaStore extends Closeable {

    /**
     * Get base location path of this table.
     */
    Path getLocationPath();

    /**
     * Get partition location path for this partition spec.
     * See {@link Catalog#getPartition}.
     *
     * @param partitionSpec partition spec should be a full spec, must be in the same order as
     *                      the partition keys of the table.
     * @return empty if table has no this partition, some if table already has partition data.
     */
    Optional<Path> getPartition(LinkedHashMap<String, String> partitionSpec) throws Exception;

    /**
     * Get Partition location path for this partition spec
     *
     * @param partitionSpec partition spec should be a full spec, must be in the same order as
     *                      the partition keys of the table.
     * @return partition path
     */
    Path getPartitionPath(LinkedHashMap<String, String> partitionSpec);

    /**
     * Create partition to meta store.
     * See {@link Catalog#createPartition}.
     *
     * @param partitionSpec partition spec should be a full spec, must be in the same order as
     *                      the partition keys of the table.
     * @param partitionPath partition location path.
     */
    void createPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath) throws Exception;
  }
}

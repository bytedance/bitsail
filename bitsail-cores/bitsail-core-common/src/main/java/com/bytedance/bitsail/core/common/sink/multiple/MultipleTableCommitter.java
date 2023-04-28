/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.core.common.sink.multiple;

import com.bytedance.bitsail.base.connector.writer.v1.WriterCommitter;
import com.bytedance.bitsail.base.extension.SupportMultipleSinkTable;
import com.bytedance.bitsail.common.catalog.table.TableId;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.core.common.sink.multiple.comittable.MultipleTableCommit;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class MultipleTableCommitter<CommitT extends Serializable> implements WriterCommitter<MultipleTableCommit<CommitT>> {

  private final SupportMultipleSinkTable<?, CommitT, ?> supplier;
  private final Map<TableId, WriterCommitter<CommitT>> multipleCommitters;
  private final BitSailConfiguration templateConfiguration;

  public MultipleTableCommitter(BitSailConfiguration templateConfiguration,
                                SupportMultipleSinkTable<?, CommitT, ?> supplier) {
    this.supplier = supplier;
    this.multipleCommitters = Maps.newConcurrentMap();
    this.templateConfiguration = templateConfiguration;
  }

  @Override
  public List<MultipleTableCommit<CommitT>> commit(List<MultipleTableCommit<CommitT>> committables) throws IOException {
    for (MultipleTableCommit<CommitT> committable : committables) {
      TableId tableId = committable.getTableId();
      WriterCommitter<CommitT> realWriterCommitter = multipleCommitters.get(tableId);
      if (Objects.isNull(realWriterCommitter)) {
        BitSailConfiguration configuration = supplier.applyTableId(templateConfiguration, tableId);
        Optional<WriterCommitter<CommitT>> committer = supplier.createCommitter(configuration);
        realWriterCommitter = committer.get();
        multipleCommitters.put(tableId, realWriterCommitter);
      }
      realWriterCommitter.commit(committable.getCommits());
    }
    return Collections.emptyList();
  }
}

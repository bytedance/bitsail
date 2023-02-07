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

package com.bytedance.bitsail.flink.core.parallelism;

import com.bytedance.bitsail.base.parallelism.ParallelismAdvice;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public enum UnionParallelismStrategy {

  MAX,

  MIN;

  public int computeUnionParallelism(List<ParallelismAdvice> readerParallelismAdvices) {
    List<Integer> parallelismList = readerParallelismAdvices.stream().map(
        ParallelismAdvice::getAdviceParallelism).collect(Collectors.toList());

    switch (this) {
      case MAX:
        return Collections.max(parallelismList);
      case MIN:
        return Collections.min(parallelismList);
      default:
        throw new UnsupportedOperationException(this.name() + " does not support computeUnionParallelism");
    }
  }

}

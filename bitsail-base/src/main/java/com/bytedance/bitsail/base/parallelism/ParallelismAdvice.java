/*
 * Copyright 2022-present ByteDance.
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

package com.bytedance.bitsail.base.parallelism;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * Created 2022/4/22
 */
@Builder
@AllArgsConstructor
@Getter
public class ParallelismAdvice {

  /**
   * when ParallelismAdvice is used as an upstream advice and enforceDownStreamChain=true,
   * the downstream writer will use this adviceParallelism as its own adviceParallelism
   */
  boolean enforceDownStreamChain;

  @Setter
  private int adviceParallelism;
}

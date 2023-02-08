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

package com.bytedance.bitsail.connector.legacy.streamingfile.common.datasink.common;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.time.format.DateTimeFormatter;

/**
 * Created 2020/3/30.
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(of = "name")
@Builder
public class PartitionInfo implements Serializable {

  private static final long serialVersionUID = 440707095804192552L;

  /**
   * The partition name like date..
   */
  private String name;

  /**
   * Partition value like 20200313
   */
  private String value;

  /**
   * Partition type
   */
  private PartitionType type;

  @JsonIgnore
  private transient DateTimeFormatter formatter;

  public void createFormatter() {
    if (PartitionType.TIME.equals(type)) {
      formatter = DateTimeFormatter.ofPattern(value);
    }
  }

}

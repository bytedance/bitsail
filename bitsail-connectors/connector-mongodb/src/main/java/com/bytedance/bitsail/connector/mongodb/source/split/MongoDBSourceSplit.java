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

package com.bytedance.bitsail.connector.mongodb.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

/**
 * Split info for MongoDB source
 */
@Setter
@Getter
public class MongoDBSourceSplit implements SourceSplit {

  public static final String SOURCE_SPLIT_PREFIX = "mongodb_source_split_";

  private String splitId;
  private Object lowerBound;
  private Object upperBound;

  public MongoDBSourceSplit() {
  }

  public MongoDBSourceSplit(String splitId, Object lowerBound, Object upperBound) {
    this.splitId = SOURCE_SPLIT_PREFIX + splitId;
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  @Override
  public String toString() {
    return String.format("{\"split_id\":\"%s\", \"lowerBound\":%s, \"upperBound\":%s}",
        splitId, lowerBound, upperBound);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MongoDBSourceSplit that = (MongoDBSourceSplit) o;
    return splitId.equals(that.splitId) && lowerBound.equals(that.lowerBound) && upperBound.equals(that.upperBound);
  }

  @Override
  public int hashCode() {
    return Objects.hash(splitId, lowerBound, upperBound);
  }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.connector.kudu.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;

import lombok.Getter;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;

import java.util.ArrayList;
import java.util.List;

// 把它搞成serializable的，并用predicates
@Getter
public class KuduSourceSplit implements SourceSplit {

  public static final String KUDU_SOURCE_SPLIT_PREFIX = "kudu_source_split_";

  private final String splitId;

  private List<KuduPredicate> predicates;

  public KuduSourceSplit(int splitId) {
    this.splitId = KUDU_SOURCE_SPLIT_PREFIX + splitId;
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  public void addPredicate(KuduPredicate predicate) {
    if (predicates == null) {
      predicates = new ArrayList<>();
    }
    predicates.add(predicate);
  }

  public void bindScanner(KuduScanner.KuduScannerBuilder builder) {
    predicates.forEach(builder::addPredicate);
  }
}

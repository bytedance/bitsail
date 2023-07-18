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

package com.bytedance.bitsail.connector.kudu.source.split;

import com.bytedance.bitsail.base.connector.reader.v1.SourceSplit;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.connector.kudu.error.KuduErrorCode;

import com.alibaba.fastjson.JSONObject;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
public class KuduSourceSplit implements SourceSplit {

  public static final String KUDU_SOURCE_SPLIT_PREFIX = "kudu_source_split_";

  private final String splitId;

  private List<byte[]> serializedPredicates = new ArrayList<>();

  /** The scan token that the split will use to scan the Kudu table. */
  private byte[] serializedScanToken;

  /** Tablet server locations which host the tablet to be scanned. */
  private String[] locations;

  public byte[] getSerializedScanToken() {
    return serializedScanToken;
  }

  public void setSerializedScanToken(byte[] serializedScanToken) {
    this.serializedScanToken = serializedScanToken;
  }

  public String[] getLocations() {
    return locations;
  }

  public void setLocations(String[] locations) {
    this.locations = locations;
  }

  public KuduSourceSplit(int splitId) {
    this.splitId = KUDU_SOURCE_SPLIT_PREFIX + splitId;
  }

  public String toFormatString(Schema schema) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("split_id", this.uniqSplitId());

    List<KuduPredicate> kuduPredicates = deserializePredicates(schema);
    properties.put("predicates", kuduPredicates.toString());

    return new JSONObject(properties).toJSONString();
  }

  @Override
  public String uniqSplitId() {
    return splitId;
  }

  public void addPredicate(KuduPredicate predicate) throws IOException {
    if (serializedPredicates == null) {
      serializedPredicates = new ArrayList<>();
    }
    serializedPredicates.add(KuduPredicate.serialize(Collections.singletonList(predicate)));
  }

  public void bindScanner(KuduScanner.KuduScannerBuilder builder, Schema schema) {
    List<KuduPredicate> kuduPredicates = deserializePredicates(schema);
    kuduPredicates.forEach(builder::addPredicate);
  }

  @VisibleForTesting
  public List<KuduPredicate> deserializePredicates(Schema schema) {
    List<KuduPredicate> kuduPredicates = new ArrayList<>();
    for (byte[] predicateBytes : serializedPredicates) {
      try {
        kuduPredicates.addAll(KuduPredicate.deserialize(schema, predicateBytes));
      } catch (IOException e) {
        throw new BitSailException(KuduErrorCode.PREDICATE_ERROR, "Failed to deserialize predicates: " + e.getCause().getMessage());
      }
    }
    return kuduPredicates;
  }

  @Override
  public boolean equals(Object obj) {
    return (obj instanceof KuduSourceSplit) && (splitId.equals(((KuduSourceSplit) obj).splitId));
  }

  @Override
  public int hashCode() {
    return splitId.hashCode() +
        ((Long) serializedPredicates.stream().map(Arrays::hashCode).mapToLong(Integer::intValue).sum()).intValue();
  }
}

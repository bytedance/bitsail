/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.connector.cdc.source.offset;

import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.cdc.error.BinlogReaderErrorCode;
import com.bytedance.bitsail.connector.cdc.option.BinlogReaderOptions;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Data
public class BinlogOffset implements Serializable {
  private static final long serialVersionUID = 1L;

  private final BinlogOffsetType offsetType;

  private final Map<String, String> props;

  public BinlogOffset(BinlogOffsetType offsetType, Map<String, String> props) {
    this.offsetType = offsetType;
    this.props = props;
  }

  public static BinlogOffset earliest() {
    return new BinlogOffset(BinlogOffsetType.EARLIEST, new HashMap<>());
  }

  public static BinlogOffset latest() {
    return new BinlogOffset(BinlogOffsetType.LATEST, new HashMap<>());
  }

  public static BinlogOffset boundless() {
    return new BinlogOffset(BinlogOffsetType.BOUNDLESS, new HashMap<>());
  }

  public static BinlogOffset specified() {
    return new BinlogOffset(BinlogOffsetType.SPECIFIED, new HashMap<>());
  }

  public static BinlogOffset createFromJobConf(BitSailConfiguration jobConf) {
    String rawOffsetType = jobConf.get(BinlogReaderOptions.INITIAL_OFFSET_TYPE).toUpperCase().trim();
    BinlogOffsetType offsetType = BinlogOffsetType.valueOf(rawOffsetType);
    switch (offsetType) {
      case LATEST:
        return latest();
      case EARLIEST:
        return earliest();
      case BOUNDLESS:
        return boundless();
      case SPECIFIED:
        Map<String, String> offsetProps = jobConf.getUnNecessaryMap(BinlogReaderOptions.INITIAL_OFFSET_PROPS);
        if (offsetProps.isEmpty()) {
          throw new BitSailException(BinlogReaderErrorCode.REQUIRED_VALUE,
              "specified binlog offset require initial binlog offset, but binlog offset property initial_offset_props is empty.");
        }
        BinlogOffset result = specified();
        result.addProps(offsetProps);
        return result;
      default:
        throw new BitSailException(BinlogReaderErrorCode.UNSUPPORTED_ERROR, "Unsupported offset type: " + rawOffsetType);
    }
  }

  public void addProps(String key, String value) {
    this.props.put(key, value);
  }

  public void addProps(Map<String, String> props) {
    this.props.putAll(props);
  }

  @Override
  public String toString() {
    return String.format("Binlog offset object with offset type: %s, properties: %s",
        offsetType,
        props.toString());
  }

}

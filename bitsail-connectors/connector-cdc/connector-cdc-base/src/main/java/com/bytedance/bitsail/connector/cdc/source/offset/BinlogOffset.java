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
import java.util.Properties;

@Data
public class BinlogOffset implements Serializable {
  private static final long serialVersionUID = 1L;

  private final BinlogOffsetType offsetType;

  private final Properties props;

  public BinlogOffset(BinlogOffsetType offsetType, Properties props) {
    this.offsetType = offsetType;
    this.props = props;
  }

  public static BinlogOffset earliest() {
    return new BinlogOffset(BinlogOffsetType.EARLIEST, new Properties());
  }

  public static BinlogOffset latest() {
    return new BinlogOffset(BinlogOffsetType.LATEST, new Properties());
  }

  public static BinlogOffset boundless() {
    return new BinlogOffset(BinlogOffsetType.BOUNDLESS, new Properties());
  }

  public static BinlogOffset specified() {
    return new BinlogOffset(BinlogOffsetType.SPECIFIED, new Properties());
  }

  public static BinlogOffset createFromJobConf(BitSailConfiguration jobConf) {
    String rawOffsetType = jobConf.getNecessaryOption(
        BinlogReaderOptions.INITIAL_OFFSET_TYPE, BinlogReaderErrorCode.REQUIRED_VALUE).toUpperCase().trim();
    BinlogOffsetType offsetType = BinlogOffsetType.valueOf(rawOffsetType);
    switch (offsetType) {
      case LATEST:
        return latest();
      case EARLIEST:
        return earliest();
      case BOUNDLESS:
        return boundless();
      case SPECIFIED:
        // currently only support mysql. Using format filename,offset
        String offsetValue = jobConf.getNecessaryOption(
            BinlogReaderOptions.INITIAL_OFFSET_VALUE, BinlogReaderErrorCode.OFFSET_VAL_ERROR);
        // TODO: make this more robust
        String filename = offsetValue.split(",")[0];
        String offset = offsetValue.split(",")[1];
        BinlogOffset result = specified();
        // TODO: move constant to common place
        result.addProps("filename", filename);
        result.addProps("offset", offset);
        return result;
      default:
        throw new BitSailException(BinlogReaderErrorCode.UNSUPPORTED_ERROR, "Unsupported offset type: " + rawOffsetType);
    }
  }

  public void addProps(String key, String value) {
    this.props.put(key, value);
  }

  @Override
  public String toString() {
    return String.format("Binlog offset object with offset type: %s, properties: %s",
        offsetType,
        props.toString());
  }

}

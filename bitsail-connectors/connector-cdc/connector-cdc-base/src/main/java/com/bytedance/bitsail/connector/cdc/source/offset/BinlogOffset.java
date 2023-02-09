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

import lombok.Data;

import java.io.Serializable;
import java.util.Properties;

@Data
public class BinlogOffset implements Serializable {
  private static final long serialVersionUID = 1L;

  private final OffsetType offsetType;

  private final Properties props;

  public BinlogOffset(OffsetType offsetType, Properties props) {
    this.offsetType = offsetType;
    this.props = props;
  }

  public static BinlogOffset earliest() {
    return new BinlogOffset(OffsetType.EARLIEST, new Properties());
  }

  public static BinlogOffset boundless() {
    return new BinlogOffset(OffsetType.BOUNDLESS, new Properties());
  }

  public enum OffsetType {
    // earliest point in binlog
    EARLIEST,
    // latest point in binlog
    LATEST,
    // specified point in the binlog file
    SPECIFIED,
    // represent an endless point, could only be the end point
    BOUNDLESS
  }

  @Override
  public String toString() {
    return String.format("Binlog offset object with offset type: %s, properties: %s",
        offsetType,
        props.toString());
  }

}

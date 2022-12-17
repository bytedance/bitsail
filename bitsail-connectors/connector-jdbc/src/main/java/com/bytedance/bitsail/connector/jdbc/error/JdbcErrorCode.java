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
 *
 * Original Files: alibaba/DataX (https://github.com/alibaba/DataX)
 * Copyright: Copyright 1999-2022 Alibaba Group Holding Ltd.
 * SPDX-License-Identifier: Apache License 2.0
 *
 * This file may have been modified by ByteDance Ltd. and/or its affiliates.
 */

package com.bytedance.bitsail.connector.jdbc.error;

import com.bytedance.bitsail.common.exception.ErrorCode;

public enum JdbcErrorCode implements ErrorCode {
  REQUIRED_VALUE("Jdbc-00", "The configuration file is lack of necessary options"),
  CONFIG_ERROR("Jdbc-01", "The configuration has wrong option"),
  CONVERT_ERROR("Jdbc-02", "Failed to convert Jdbc result to row");
  private final String code;

  private final String describe;

  JdbcErrorCode(String code, String describe) {
    this.code = code;
    this.describe = describe;
  }

  @Override
  public String getCode() {
    return null;
  }

  @Override
  public String getDescription() {
    return null;
  }
}

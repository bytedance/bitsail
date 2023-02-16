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

package com.bytedance.bitsail.component.format.debezium;

@SuppressWarnings("checkstyle:MagicNumber")
public class DebeziumRowMessage {
  public static int ROW_SIZE = 7;

  public static int DATABASE_INDEX = 0;

  public static int TABLE_INDEX = 1;

  public static int ID_INDEX = 2;

  public static int TIMESTAMP_INDEX = 3;

  public static int DDL_FLAG_INDEX = 4;

  public static int VERSION_INDEX = 5;

  public static int VALUE_INDEX = 6;
}

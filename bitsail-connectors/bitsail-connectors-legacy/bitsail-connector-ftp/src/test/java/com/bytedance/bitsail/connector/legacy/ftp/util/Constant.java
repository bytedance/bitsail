/*
 * Copyright 2022-present, Bytedance Ltd.
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

package com.bytedance.bitsail.connector.legacy.ftp.util;

public class Constant {
  public static final int FTP_PORT = 60021;
  public static final int SFTP_PORT = 22;
  public static final int DEFAULT_TIMEOUT = 5000;

  public static final String LOCALHOST = "localhost";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String UPLOAD = "/upload/";
  public static final String SUCCESS_TAG = "_SUCCESS";
}

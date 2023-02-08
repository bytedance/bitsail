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

package com.bytedance.bitsail.connector.doris;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Objects;

@Data
@NoArgsConstructor
public class DorisConnectionHolder {
  private Connection dorisConnection;
  private Statement statement;

  public DorisConnectionHolder(Connection dorisConnection, Statement statement) {
    this.dorisConnection = dorisConnection;
    this.statement = statement;
  }

  public void closeDorisConnection() throws IOException {
    try {
      if (Objects.nonNull(statement)) {
        statement.close();
      }
      if (Objects.nonNull(dorisConnection)) {
        dorisConnection.close();
      }
      statement = null;
      dorisConnection = null;
    } catch (Exception e) {
      throw new IOException("failed to close statement or doris connection", e);
    }
  }
}

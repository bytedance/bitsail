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

package com.bytedance.bitsail.test.integration.cdc.mysql.container.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

@Getter
@AllArgsConstructor
public class TestDatabase {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatabase.class);

  private final MySQLContainer mySQLContainer;

  private final String databaseName;

  private final String username;

  private final String password;

  public void executeSql(String sql) {
    try {
      Connection connection = DriverManager.getConnection(
          mySQLContainer.getJdbcUrl(), username, password);
      Statement statement = connection.createStatement();
      LOG.info("executing sql: {}", sql);
      boolean result = statement.execute(sql);
      LOG.info("executing sql completed: {}", result);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
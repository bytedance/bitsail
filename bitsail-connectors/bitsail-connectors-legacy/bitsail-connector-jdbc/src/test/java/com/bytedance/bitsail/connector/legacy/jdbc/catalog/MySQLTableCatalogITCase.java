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

package com.bytedance.bitsail.connector.legacy.jdbc.catalog;

import com.bytedance.bitsail.common.catalog.table.CatalogTable;
import com.bytedance.bitsail.common.catalog.table.CatalogTableDefinition;
import com.bytedance.bitsail.connector.legacy.jdbc.container.MySQLContainerMariadbAdapter;
import com.bytedance.bitsail.connector.legacy.jdbc.converter.JdbcTypeInfoConverter;

import org.apache.commons.collections.CollectionUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.util.stream.Stream;

public class MySQLTableCatalogITCase {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLTableCatalogITCase.class);

  private static final String MYSQL_DOCKER_IMAGER = "mysql:8.0.29";

  private MySQLContainer<?> container;

  private static final String TABLE = "jdbc_dynamic_table";
  private static final String DATABASE = "test";

  @Before
  public void before() {
    container = new MySQLContainerMariadbAdapter<>(DockerImageName.parse(MYSQL_DOCKER_IMAGER))
        .withUrlParam("permitMysqlScheme", null)
        .withInitScript("scripts/fake_to_jdbc_sink.sql")
        .withLogConsumer(new Slf4jLogConsumer(LOG));

    Startables.deepStart(Stream.of(container)).join();
  }

  @After
  public void after() {
    container.close();
  }

  @Test
  public void testGetCatalogTable() {
    MySQLTableCatalog catalog = MySQLTableCatalog
        .builder()
        .url(container.getJdbcUrl())
        .table(TABLE)
        .database(DATABASE)
        .username(container.getUsername())
        .password(container.getPassword())
        .build();

    catalog.open(new JdbcTypeInfoConverter("mysql"));

    CatalogTableDefinition catalogTableDefinition = catalog.createCatalogTableDefinition();
    CatalogTable catalogTable = catalog.getCatalogTable(catalogTableDefinition);

    Assert.assertNotNull(catalogTable.getCatalogTableSchema());
    Assert.assertEquals(CollectionUtils.size(catalogTable.getCatalogTableSchema().getColumns()), 21);
  }
}

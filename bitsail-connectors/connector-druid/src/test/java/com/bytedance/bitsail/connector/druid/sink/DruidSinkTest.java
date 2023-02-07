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

package com.bytedance.bitsail.connector.druid.sink;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.model.ColumnInfo;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import static com.bytedance.bitsail.common.option.WriterOptions.BaseWriterOptions.COLUMNS;
import static com.bytedance.bitsail.connector.druid.error.DruidErrorCode.REQUIRED_VALUE;
import static com.bytedance.bitsail.connector.druid.option.DruidWriterOptions.COORDINATOR_URL;
import static com.bytedance.bitsail.connector.druid.option.DruidWriterOptions.DATASOURCE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class DruidSinkTest {
  private final BitSailConfiguration mockConfig = mock(BitSailConfiguration.class);
  private DruidSink<Serializable> druidSink;

  @Before
  public void setup() {
    druidSink = new DruidSink<>();
  }

  @Test
  public void testGetWriterName() {
    assertEquals("druid", druidSink.getWriterName());
  }

  @Test
  public void testConfigure() {

    // Act
    druidSink.configure(null, mockConfig);

    // Assert
    assertEquals(mockConfig, druidSink.getWriterConf());
  }

  @Test
  public void testCreateWriter() throws IOException {

    // Arrange
    druidSink.configure(null, mockConfig);
    final List<ColumnInfo> columns = Collections.emptyList();
    doReturn(columns).when(mockConfig).getNecessaryOption(COLUMNS, REQUIRED_VALUE);
    doReturn("localhost").when(mockConfig).getNecessaryOption(COORDINATOR_URL, REQUIRED_VALUE);
    doReturn("testDatasource").when(mockConfig).getNecessaryOption(DATASOURCE, REQUIRED_VALUE);

    // Act; Assert
    assertEquals(columns, ((DruidWriter) druidSink.createWriter(null)).getColumnInfos());
  }
}

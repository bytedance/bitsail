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
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.type.filemapping.FileMappingTypeInfoConverter;

import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DruidWriterTest {
  private final TypeInfoConverter converter = new FileMappingTypeInfoConverter("druid");
  private DruidWriter druidWriter;

  @Before
  public void setup() throws IOException, URISyntaxException {
    final BitSailConfiguration configuration = BitSailConfiguration.from(new File(
        Paths.get(getClass().getClassLoader().getResource("druid_writer.json").toURI()).toString()));

    druidWriter = new DruidWriter(configuration, converter);
  }

  @Test
  public void testWrite() {

    // Arrange
    final Row row1 = new Row(new Object[]{"string1", 123, 123L, 123.45f, 123.45});
    final Row row2 = new Row(new Object[]{"string2", 456, 456L, 456.78f, 456.78});

    // Act
    druidWriter.write(row1);
    druidWriter.write(row2);

    // Assert
    final long processTime = druidWriter.getProcessTime();
    final String expect = "string1,123,123,123.45,123.45," + processTime +
            "\nstring2,456,456,456.78,456.78," + processTime + "\n";
    assertEquals(expect, druidWriter.getData().toString());
  }

  @Test
  public void testJobPayload() throws URISyntaxException, IOException {

    // Arrange
    final DruidWriter prePopulatedDruidWriter = getPrePopulatedDruidWriter(null);
    final ParallelIndexIOConfig ioConfig = prePopulatedDruidWriter.provideDruidIOConfig(prePopulatedDruidWriter.getData());
    final ParallelIndexSupervisorTask indexTask = prePopulatedDruidWriter.provideIndexTask(ioConfig);

    // Act
    final String inputJSON = prePopulatedDruidWriter.provideInputJSONString(indexTask);

    // Assert
    String expectedTaskJson = new String(Files.readAllBytes(
        Paths.get(getClass().getClassLoader().getResource("expectedTask.json").toURI())));
    assertEquals(expectedTaskJson, inputJSON);
  }

  @Test
  public void testFlush() throws IOException, URISyntaxException {

    // Arrange
    final HttpURLConnection mockUrlCon = mock(HttpURLConnection.class);
    final DruidWriter prePopulatedDruidWriter = getPrePopulatedDruidWriter(mockUrlCon);
    final OutputStream mockOs = mock(OutputStream.class);
    final InputStream mockIs = mock(InputStream.class);
    doReturn(mockOs).when(mockUrlCon).getOutputStream();
    doReturn(mockIs).when(mockUrlCon).getInputStream();
    doReturn(-1).when(mockIs).read(any(byte[].class), anyInt(), anyInt());

    // Act
    prePopulatedDruidWriter.flush(false);

    // Assert
    String expectedTaskJson = new String(Files.readAllBytes(
        Paths.get(getClass().getClassLoader().getResource("expectedTask.json").toURI())));
    final byte[] expectedInput = expectedTaskJson.getBytes(StandardCharsets.UTF_8);
    verify(mockOs, times(1)).write(expectedInput, 0, expectedInput.length);
  }

  @Test
  public void testPrepareCommitReturnNull() {
    assertNull(druidWriter.prepareCommit());
  }

  private DruidWriter getPrePopulatedDruidWriter(final HttpURLConnection connection) throws URISyntaxException, IOException {
    final BitSailConfiguration configuration = BitSailConfiguration.from(new File(
        Paths.get(getClass().getClassLoader().getResource("druid_writer.json").toURI()).toString()));
    final long processTime = 1668694453938L;
    final DruidWriter druidWriterWithMockInjection = new DruidWriter(configuration, converter, connection, processTime);
    final Row row1 = new Row(new Object[]{"string1", 123, 123L, 123.45f, 123.45});
    druidWriterWithMockInjection.write(row1);
    return druidWriterWithMockInjection;
  }
}

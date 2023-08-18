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

package com.bytedance.bitsail.test.integration.elasticsearch;

import com.bytedance.bitsail.base.connector.writer.v1.Writer;
import com.bytedance.bitsail.base.connector.writer.v1.state.EmptyState;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.row.Row;
import com.bytedance.bitsail.common.row.RowKind;
import com.bytedance.bitsail.common.type.BitSailTypeInfoConverter;
import com.bytedance.bitsail.common.type.TypeInfoConverter;
import com.bytedance.bitsail.common.typeinfo.RowTypeInfo;
import com.bytedance.bitsail.common.typeinfo.TypeInfoUtils;
import com.bytedance.bitsail.connector.elasticsearch.option.ElasticsearchOptions;
import com.bytedance.bitsail.connector.elasticsearch.sink.ElasticsearchWriter;
import com.bytedance.bitsail.connector.elasticsearch.utils.ElasticsearchUtils;
import com.bytedance.bitsail.test.integration.elasticsearch.container.ElasticsearchCluster;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.google.common.collect.Maps;
import org.apache.commons.collections.MapUtils;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ElasticsearchWriterITCase {

  private static ElasticsearchCluster cluster;
  private static final String INDEX_NAME = "elasticsearch_index";

  private RestHighLevelClient client;
  private BitSailConfiguration jobConf;

  @BeforeClass
  public static void beforeClass() throws Exception {
    cluster = new ElasticsearchCluster();
    cluster.startService();
    cluster.checkClusterHealth();
  }

  @Before
  public void before() {
    cluster.resetIndex(INDEX_NAME);
    jobConf = BitSailConfiguration.newDefault();
    jobConf.setWriter(ElasticsearchOptions.HOSTS, cluster.getHttpHostAddress());
    client = new RestHighLevelClient(ElasticsearchUtils.prepareRestClientBuilder(jobConf.getSubConfiguration(WriterOptions.JOB_WRITER)));
  }

  @After
  public void after() throws Exception {
    client.close();
  }

  @AfterClass
  public static void afterClass() {
    cluster.close();
  }

  @Test
  public <CommitT, WriterStateT> void testWriteWithRowKind() throws Exception {
    BitSailConfiguration writerConfiguration = JobConfUtils.fromClasspath("es_writer_parameter_test.json");
    writerConfiguration.merge(jobConf, true);
    writerConfiguration.setWriter(ElasticsearchOptions.INDEX, INDEX_NAME);
    TypeInfoConverter fileMappingTypeInfoConverter = new BitSailTypeInfoConverter();
    RowTypeInfo rowTypeInfo = TypeInfoUtils.getRowTypeInfo(fileMappingTypeInfoConverter, writerConfiguration.get(WriterOptions.BaseWriterOptions.COLUMNS));
    ElasticsearchWriter<CommitT> writer = new ElasticsearchWriter<>(
        new WriterMockContext(rowTypeInfo),
        BitSailConfiguration.newDefault(),
        writerConfiguration.getSubConfiguration(WriterOptions.JOB_WRITER));

    long id = 1;
    writer.write(mockRow(RowKind.INSERT, id));
    writer.flush(false);
    checkDocumentExistsOrNot(client, INDEX_NAME, id, true);
    writer.write(mockRow(RowKind.DELETE, id));
    writer.flush(false);
    checkDocumentExistsOrNot(client, INDEX_NAME, id, false);
    id = 2;
    writer.write(mockRow(RowKind.UPDATE_AFTER, id));
    writer.flush(false);
    checkDocumentExistsOrNot(client, INDEX_NAME, id, true);
    writer.write(mockRow(RowKind.UPDATE_BEFORE, id));
    writer.flush(false);
    checkDocumentExistsOrNot(client, INDEX_NAME, id, false);

    //Null value could write in INSERT MODE
    Object nullId = null;
    writer.write(mockRow(RowKind.INSERT, nullId));
    writer.flush(false);
  }

  private static void checkDocumentExistsOrNot(RestHighLevelClient client,
                                               String index,
                                               Object id,
                                               boolean exists) throws Exception {
    GetRequest getRequest = new GetRequest()
        .index(index)
        .id(id.toString());
    GetResponse response = client.get(getRequest, RequestOptions.DEFAULT);

    Map<String, Object> source = response.getSourceAsMap();
    if (exists) {
      Assert.assertTrue(MapUtils.isNotEmpty(source));
    } else {
      Assert.assertTrue(MapUtils.isEmpty(source));
    }
  }

  private static Row mockRow(RowKind rowKind, Object id) {
    Map<String, String> values = Maps.newHashMap();
    values.put("k", "v");
    Row row = new Row(new Object[] {
        id, "varchar", "text", "bigint", "20220810"
    });
    row.setKind(rowKind);
    return row;
  }

  public static class WriterMockContext implements Writer.Context<EmptyState> {

    private RowTypeInfo rowTypeInfo;

    public WriterMockContext(RowTypeInfo rowTypeInfo) {
      this.rowTypeInfo = rowTypeInfo;
    }

    @Override
    public RowTypeInfo getRowTypeInfo() {
      return rowTypeInfo;
    }

    @Override
    public int getIndexOfSubTaskId() {
      return 0;
    }

    @Override
    public boolean isRestored() {
      return false;
    }

    @Override
    public List<EmptyState> getRestoreStates() {
      return null;
    }
  }
}


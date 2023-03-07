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

package com.bytedance.bitsail.test.integration.legacy.mongodb;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.legacy.fake.option.FakeReaderOptions;
import com.bytedance.bitsail.connector.legacy.mongodb.option.MongoDBReaderOptions;
import com.bytedance.bitsail.connector.legacy.mongodb.option.MongoDBWriterOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.legacy.mongodb.container.TestMongoDBContainer;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoDBConnectorITCase extends AbstractIntegrationTest {

  private static final int TOTAL_COUNT = 300;
  private static final String DB_NAME = "test_db";

  private static final String SOURCE_COLLECTION = "test_collection_source";
  private static final String SINK_COLLECTION = "test_collection_sink";

  private static TestMongoDBContainer mongoDBContainer;
  private static String mongodbConnStr;

  @BeforeClass
  public static void initMongodbCluster() {
    mongoDBContainer = new TestMongoDBContainer();
    mongoDBContainer.start();
    mongoDBContainer.createCollection(DB_NAME, SOURCE_COLLECTION);
    mongoDBContainer.createCollection(DB_NAME, SINK_COLLECTION);
    mongodbConnStr = mongoDBContainer.getConnectionStr();
  }

  @AfterClass
  public static void closeMongodbCluster() {
    if (mongoDBContainer != null) {
      mongoDBContainer.close();
      mongoDBContainer = null;
    }
  }

  @Test
  public void testFakeToMongoDB() throws Exception {
    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("fake_to_mongodb.json");

    jobConf.set(FakeReaderOptions.TOTAL_COUNT, TOTAL_COUNT);
    jobConf.set(MongoDBWriterOptions.CLIENT_MODE, "url");
    jobConf.set(MongoDBWriterOptions.MONGO_URL, mongodbConnStr + "/" + DB_NAME);
    jobConf.set(MongoDBWriterOptions.DB_NAME, DB_NAME);
    jobConf.set(MongoDBWriterOptions.COLLECTION_NAME, SINK_COLLECTION);

    submitJob(jobConf);

    Assert.assertEquals(TOTAL_COUNT, mongoDBContainer.countDocuments(DB_NAME, SINK_COLLECTION));
  }

  @Test
  public void testMongoDBToPrint() throws Exception {
    insertDocument();

    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("mongodb_to_print.json");

    jobConf.set(MongoDBReaderOptions.HOST, "localhost");
    jobConf.set(MongoDBReaderOptions.PORT, getPort(mongodbConnStr));
    jobConf.set(MongoDBReaderOptions.DB_NAME, DB_NAME);
    jobConf.set(MongoDBReaderOptions.COLLECTION_NAME, SOURCE_COLLECTION);
    jobConf.set(MongoDBReaderOptions.SPLIT_PK, "_id");

    submitJob(jobConf);
  }

  private void insertDocument() {
    List<Map<String, Object>> docFieldList = new ArrayList<>();
    for (int i = 0; i < TOTAL_COUNT; ++i) {
      Map<String, Object> docField = new HashMap<>();
      docField.put("string_field", "str_" + i);
      docField.put("int_field", i);
      docFieldList.add(docField);
    }
    mongoDBContainer.insertDocuments(DB_NAME, SOURCE_COLLECTION, docFieldList);
  }

  private int getPort(String url) {
    return Integer.parseInt(url.split(":")[2]);
  }
}

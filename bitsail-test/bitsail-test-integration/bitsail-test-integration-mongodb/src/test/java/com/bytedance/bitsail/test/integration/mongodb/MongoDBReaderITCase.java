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

package com.bytedance.bitsail.test.integration.mongodb;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.mongodb.option.MongoDBReaderOptions;
import com.bytedance.bitsail.test.integration.AbstractIntegrationTest;
import com.bytedance.bitsail.test.integration.mongodb.container.TestMongoDBContainer;
import com.bytedance.bitsail.test.integration.utils.JobConfUtils;

import com.mongodb.BasicDBObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Integration test for MongoDB reader.
 */
public class MongoDBReaderITCase extends AbstractIntegrationTest {

  private static final int TOTAL_COUNT = 300;
  private static final String DB_NAME = "test_db";
  private static final String COLLECTION_NAME = "test_collection";

  private TestMongoDBContainer mongoContainer;
  private String mongoConnStr;

  @Before
  public void startContainer() {
    mongoContainer = new TestMongoDBContainer();
    mongoContainer.start();
    mongoContainer.createCollection(DB_NAME, COLLECTION_NAME);
    mongoConnStr = mongoContainer.getConnectionStr();
  }

  @Test
  public void testMongoDBToPrint() throws Exception {
    insertDocument();

    BitSailConfiguration jobConf = JobConfUtils.fromClasspath("mongodb_to_print.json");

    jobConf.set(MongoDBReaderOptions.HOST, getHost(mongoConnStr));
    jobConf.set(MongoDBReaderOptions.PORT, getPort(mongoConnStr));
    jobConf.set(MongoDBReaderOptions.DB_NAME, DB_NAME);
    jobConf.set(MongoDBReaderOptions.COLLECTION_NAME, COLLECTION_NAME);

    submitJob(jobConf);
  }

  @After
  public void closeContainer() {
    if (Objects.nonNull(mongoContainer)) {
      mongoContainer.close();
      mongoContainer = null;
    }
  }

  private void insertDocument() {
    List<Map<String, Object>> docFieldList = new ArrayList<>();

    Date currDate = new Date();
    List<String> list = Arrays.asList("one", "two");
    Map<String, Object> map = new HashMap<>(8);

    for (int i = 0; i < TOTAL_COUNT; ++i) {
      Map<String, Object> docField = new HashMap<>();
      docField.put("int_field", i);
      docField.put("string_field", "str_" + i);
      docField.put("date_field", currDate);
      docField.put("array_field", list);

      map.clear();
      map.put("age", i + 10);
      map.put("name", "name_" + i);
      docField.put("object_field", new BasicDBObject(map));

      docFieldList.add(docField);
    }
    mongoContainer.insertDocuments(DB_NAME, COLLECTION_NAME, docFieldList);
  }

  private String getHost(String url) {
    return url.split(":")[1].replace("/", "");
  }

  private int getPort(String url) {
    return Integer.parseInt(url.split(":")[2]);
  }
}

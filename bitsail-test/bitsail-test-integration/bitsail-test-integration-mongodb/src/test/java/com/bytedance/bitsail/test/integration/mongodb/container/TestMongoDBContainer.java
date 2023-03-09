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

package com.bytedance.bitsail.test.integration.mongodb.container;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MongoDBContainer;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@NoArgsConstructor
public class TestMongoDBContainer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TestMongoDBContainer.class);

  private static final String MONGO_VERSION = "mongo:4.0.10";

  private MongoDBContainer container;

  @Getter
  private String connectionStr;
  private MongoClient mongoClient;

  private static boolean notExist(Iterable<String> names, String targetName) {
    for (String name : names) {
      if (name.equals(targetName)) {
        return false;
      }
    }
    return true;
  }

  private static Document makeDocument(Map<String, Object> fields) {
    Document document = new Document();
    document.putAll(fields);
    return document;
  }

  public void start() {
    container = new MongoDBContainer(MONGO_VERSION);
    container.start();
    LOG.info("MongoDB container started.");

    initMongoClient();
  }

  @Override
  public void close() {
    container.close();
  }

  public void createCollection(String databaseName, String collectionName) {
    initMongoClient();
    MongoDatabase db = mongoClient.getDatabase(databaseName);
    db.createCollection(collectionName);

    if (notExist(mongoClient.listDatabaseNames(), databaseName)) {
      throw new RuntimeException("Failed to create database in mongodb container.");
    }

    if (notExist(db.listCollectionNames(), collectionName)) {
      throw new RuntimeException("Failed to create collection in mongodb container.");
    }
    LOG.info("Successfully create {}:{}", databaseName, collectionName);
  }

  public long countDocuments(String databaseName, String collectionName) {
    initMongoClient();
    MongoDatabase db = mongoClient.getDatabase(databaseName);
    MongoCollection<Document> collection = db.getCollection(collectionName);
    return collection.countDocuments();
  }

  /**
   * Insert documents, make sure database and collection are created.
   */
  public void insertDocuments(String databaseName, String collectionName, List<Map<String, Object>> documents) {
    initMongoClient();
    MongoDatabase db = mongoClient.getDatabase(databaseName);
    MongoCollection<Document> collection = db.getCollection(collectionName);

    List<Document> documentList = documents.stream()
        .map(TestMongoDBContainer::makeDocument)
        .collect(Collectors.toList());

    collection.insertMany(documentList);

    LOG.info("Successfully insert {} documents to {}:{}", documentList.size(), databaseName, collectionName);
  }

  private void initMongoClient() {
    if (Objects.isNull(mongoClient)) {
      connectionStr = container.getConnectionString();
      mongoClient = MongoClients.create(connectionStr);
      LOG.info("Internal mongodb client initialized.");
    }
  }
}

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

package com.bytedance.bitsail.connector.mongodb.config;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.connector.mongodb.error.MongoDBErrorCode;
import com.bytedance.bitsail.connector.mongodb.option.MongoDBReaderOptions;

import com.mongodb.MongoClientOptions;
import com.mongodb.WriteConcern;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Setter
@Getter
@Builder
@AllArgsConstructor
public class MongoDBConnConfig implements Serializable {

  private CLIENT_MODE clientMode;
  private MongoDBConnOptions options;
  private String mongoUrl;
  private String hostsStr;
  private String host;
  private int port;
  private String collectionName;
  private String dbName;
  private String userName;
  private String password;
  private String authDbName;
  private WRITE_MODE writeMode;

  /**
   * Init the MongoDB connection config from the job config.
   *
   * @param jobConf job config instance
   * @return instance of MongoDB connection config
   */
  public static MongoDBConnConfig initMongoConnConfig(BitSailConfiguration jobConf) {
    String hostsStr = "";
    String host = "";
    int port = 0;
    if (jobConf.fieldExists(MongoDBReaderOptions.HOSTS_STR)) {
      hostsStr = jobConf.getNecessaryOption(MongoDBReaderOptions.HOSTS_STR, MongoDBErrorCode.REQUIRED_VALUE);
    } else {
      host = jobConf.getNecessaryOption(MongoDBReaderOptions.HOST, MongoDBErrorCode.REQUIRED_VALUE);
      port = jobConf.getNecessaryOption(MongoDBReaderOptions.PORT, MongoDBErrorCode.REQUIRED_VALUE);
    }

    String dbName = jobConf.getNecessaryOption(MongoDBReaderOptions.DB_NAME, MongoDBErrorCode.REQUIRED_VALUE);
    String collectionName = jobConf.getNecessaryOption(MongoDBReaderOptions.COLLECTION_NAME, MongoDBErrorCode.REQUIRED_VALUE);
    String userName = jobConf.get(MongoDBReaderOptions.USER_NAME);
    String password = jobConf.get(MongoDBReaderOptions.PASSWORD);
    String authDbName = jobConf.getUnNecessaryOption(MongoDBReaderOptions.AUTH_DB_NAME, null);
    return MongoDBConnConfig.builder()
        .authDbName(authDbName)
        .hostsStr(hostsStr)
        .host(host)
        .port(port)
        .dbName(dbName)
        .collectionName(collectionName)
        .userName(userName)
        .password(password)
        .build();
  }

  public MongoClientOptions getClientOption() {
    MongoClientOptions.Builder builder = new MongoClientOptions.Builder();
    builder.connectionsPerHost(options.getConnectionsPerHost());
    builder.threadsAllowedToBlockForConnectionMultiplier(options.getThreadsAllowedToBlockForConnectionMultiplier());
    builder.connectTimeout(options.getConnectTimeout());
    builder.maxWaitTime(options.getMaxWaitTime());
    builder.socketTimeout(options.getSocketTimeout());
    builder.writeConcern(new WriteConcern(options.getWriteConcern()));
    return builder.build();
  }

  /**
   * Client mode for MongoDB
   */
  public enum CLIENT_MODE {

    /**
     * URL mode
     */
    URL,
    /**
     * Credential host mode
     */
    HOST_WITH_CREDENTIAL,
    /**
     * No-Credential host mode
     */
    HOST_WITHOUT_CREDENTIAL
  }

  /**
   * Write mode for MongoDB
   */
  public enum WRITE_MODE {
    /**
     * Insert mode
     */
    INSERT,
    /**
     * Overwrite mode
     */
    OVERWRITE
  }
}


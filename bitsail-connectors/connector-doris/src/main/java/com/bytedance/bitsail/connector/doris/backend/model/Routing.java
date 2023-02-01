/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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

package com.bytedance.bitsail.connector.doris.backend.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * present an Doris BE address.
 */
public class Routing {
  private static final Logger LOGGER = LoggerFactory.getLogger(Routing.class);
  private String host;
  private int port;

  public Routing(String routing) throws IllegalArgumentException {
    parseRouting(routing);
  }

  private void parseRouting(String routing) throws IllegalArgumentException {
    LOGGER.debug("Parse Doris BE address: '{}'.", routing);
    String[] hostPort = routing.split(":");
    if (hostPort.length != 2) {
      String errMsg = String.format("Format of Doris BE address is illegal, routing=%s", routing);
      LOGGER.error(errMsg);
      throw new IllegalArgumentException(errMsg);
    }
    this.host = hostPort[0];
    try {
      this.port = Integer.parseInt(hostPort[1]);
    } catch (NumberFormatException e) {
      String errMsg = String.format("Failed to parse Doris BE's hostPort, host=%s, Port=%s", hostPort[0], hostPort[1]);
      LOGGER.error(errMsg);
      throw new IllegalArgumentException(errMsg, e);
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  @Override
  public String toString() {
    return "Doris BE{host='" + host + '\'' + ", port=" + port + '}';
  }
}

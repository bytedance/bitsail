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

package com.bytedance.bitsail.connector.doris.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Be response model
 **/
@JsonIgnoreProperties(ignoreUnknown = true)
public class Backend {

  @JsonProperty(value = "backends")
  private List<BackendRow> backends;

  public List<BackendRow> getBackends() {
    return backends;
  }

  public void setBackends(List<BackendRow> backends) {
    this.backends = backends;
  }

  public static class BackendRow {
    @JsonProperty("ip")
    public String ip;
    @JsonProperty("http_port")
    public int httpPort;
    @JsonProperty("is_alive")
    public boolean isAlive;

    public String getIp() {
      return ip;
    }

    public void setIp(String ip) {
      this.ip = ip;
    }

    public int getHttpPort() {
      return httpPort;
    }

    public void setHttpPort(int httpPort) {
      this.httpPort = httpPort;
    }

    public boolean isAlive() {
      return isAlive;
    }

    public void setAlive(boolean alive) {
      isAlive = alive;
    }
  }
}

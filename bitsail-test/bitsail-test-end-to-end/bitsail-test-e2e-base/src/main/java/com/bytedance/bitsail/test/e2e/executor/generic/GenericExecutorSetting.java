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

package com.bytedance.bitsail.test.e2e.executor.generic;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class GenericExecutorSetting {
  private static final Logger LOG = LoggerFactory.getLogger(GenericExecutorSetting.class);

  @JsonProperty(value = "name", required = true)
  private String executorName;

  @JsonProperty(value = "executor-image", required = true)
  private String executorImage;

  @JsonProperty("client-module")
  private String clientModule;

  @JsonProperty("core-module")
  private String coreModule;

  @JsonProperty("additional-files")
  @JsonDeserialize(using = TransferableFileDeserializer.class)
  private List<TransferableFile> additionalFiles;

  @JsonProperty(value = "exec-commands", required = true)
  private List<String> execCommands;

  @JsonProperty("failure-handle-commands")
  private List<String> failureHandleCommands;

  @JsonProperty("global-job-config")
  private String globalJobConf;

  public BitSailConfiguration getGlobalJobConf() {
    if (globalJobConf == null) {
      return BitSailConfiguration.newDefault();
    }
    return BitSailConfiguration.from(globalJobConf);
  }

  /**
   * Initialize a {@link GenericExecutorSetting} from setting files.
   *
   * @param settingFilePath An executor setting file.
   */
  public static GenericExecutorSetting initFromFile(String settingFilePath) {
    try {
      File settingFile = new File(settingFilePath);
      String settingStr = IOUtils.toString(new FileInputStream(settingFile));
      return new ObjectMapper().readValue(settingStr, GenericExecutorSetting.class);
    } catch (Exception e) {
      LOG.error("Failed to initialize generic executor setting.", e);
      throw new RuntimeException("Failed to initialize generic executor setting.", e);
    }
  }

  static class TransferableFileDeserializer extends JsonDeserializer<List<TransferableFile>> {
    @Override
    public List<TransferableFile> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      return p.readValueAs(new TypeReference<List<TransferableFile>>() {
      });
    }
  }
}

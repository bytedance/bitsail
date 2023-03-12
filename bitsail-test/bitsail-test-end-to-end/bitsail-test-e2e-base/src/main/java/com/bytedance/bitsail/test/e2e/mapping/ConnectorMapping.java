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

package com.bytedance.bitsail.test.e2e.mapping;

import com.bytedance.bitsail.base.packages.Plugin;
import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.common.util.JsonSerializer;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.executor.AbstractExecutor;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ConnectorMapping {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectorMapping.class);

  private final List<ConnectorInfo> connectorInfoList;

  private static ConnectorMapping INSTANCE;

  public static ConnectorMapping getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new ConnectorMapping();
    }
    return INSTANCE;
  }

  /**
   * Get lib for connector.
   */
  public ConnectorLib getLibByClassName(String className) throws FileNotFoundException {
    for (ConnectorInfo connectorInfo : connectorInfoList) {
      if (connectorInfo.match(className)) {
        ConnectorLib lib = connectorInfo.getLib();
        lib.validate();
        return lib;
      }
    }
    throw new FileNotFoundException("Failed to find libs for class " + className);
  }

  private ConnectorMapping() {
    connectorInfoList = new ArrayList<>();
    addConnectorV1();
  }

  /**
   * Read all connector-v1 mapping files.
   */
  public void addConnectorV1() {
    String localDir = AbstractExecutor.getLocalRootDir();

    Path connectorsPath = Paths.get(localDir, "bitsail-connectors");
    File[] connectorModules = connectorsPath.toFile().listFiles((dir, name) -> name.startsWith("connector-"));
    Preconditions.checkNotNull(connectorModules, "Failed to locate V2 connectors.");
    for (File connector : connectorModules) {
      // try to find mapping file
      String connectorPath = connector.toPath().toAbsolutePath().toString();
      File resources = Paths.get(connectorPath, "src", "main", "resources").toFile();
      File[] mappingFiles = resources.listFiles((dir, name) ->
          name.startsWith("bitsail-connector-unified-") && name.endsWith(".json"));

      // not found
      if (mappingFiles == null || mappingFiles.length == 0) {
        continue;
      }

      // found
      File mappingFile = mappingFiles[0];
      try {
        Plugin plugin = JsonSerializer
            .deserialize(
                Files.readAllBytes(mappingFile.toPath()),
                Plugin.class
            );
        connectorInfoList.add(new ConnectorInfo(plugin, connectorPath, mappingFile.toPath().toAbsolutePath().toString()));
      } catch (IOException e) {
        LOG.warn("Failed to load plugin mapping from {}", connector.getName(), e);
      }
    }
  }

  @AllArgsConstructor
  static class ConnectorInfo {
    private final Plugin plugin;        // content of mapping file
    private final String connectorPath; // module path
    private final String mappingPath;   // path of mapping file

    public boolean match(String canonicalName) {
      if (canonicalName == null) {
        return false;
      }

      if (canonicalName.equals(plugin.getClassName())) {
        return true;
      }

      List<String> classNames = plugin.getClassNames();
      return classNames != null && classNames.contains(canonicalName);
    }

    public ConnectorLib getLib() {
      List<String> libFiles = plugin.getLibs().stream().map(lib ->
          Paths.get(connectorPath, "target",
              StringUtils.replace(plugin.getLibs().get(0),
                  "${version}",
                  VersionHolder.getHolder().getBuildVersion())).toAbsolutePath().toString()
      ).collect(Collectors.toList());
      return new ConnectorLib(plugin.getPluginName(), mappingPath, libFiles);
    }
  }

  @AllArgsConstructor
  @Getter
  public static class ConnectorLib {
    String name;
    String mappingFile;
    List<String> libFiles;

    public void validate() throws FileNotFoundException {
      if (!(new File(mappingFile).exists())) {
        throw new FileNotFoundException("Failed to find mapping file: " + mappingFile);
      }
      for (String libFile : libFiles) {
        if (!(new File(libFile).exists())) {
          throw new FileNotFoundException("Failed to find lib file: " + libFile);
        }
      }
    }
  }
}

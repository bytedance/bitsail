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

package com.bytedance.bitsail.entry.flink.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

public class FlinkPackageResolver {
  private static final String FLINK_LIB_DIR = "lib";
  private static final String FLINK_CONF_DIR = "conf";
  public static final String FLINK_LIB_DIST_JAR_NAME = "flink-dist";
  public static final String FLINK_CONF_FILE = "flink-conf.yaml";
  public static final String FLINK_LOG_FILE_PREFIX = "log4j";
  public static final String ENV_PROP_FLINK_CONF_DIR = "FLINK_CONF_DIR";
  private static final Logger LOG = LoggerFactory.getLogger(FlinkPackageResolver.class);

  public static Path getFlinkConfDir(Path rootDir) {
    return rootDir.resolve(FLINK_CONF_DIR);
  }

  public static Path getFlinkLibDir(Path rootDir) {
    return rootDir.resolve(FLINK_LIB_DIR);
  }

  public static Configuration loadFlinkConfiguration(Path flinkConfDir) {
    if (!Files.isDirectory(flinkConfDir) || !(new File(flinkConfDir.toFile(), FLINK_CONF_FILE)).exists()) {
      LOG.info("No flink configuration file from path: {}. Initiate a new configuration", flinkConfDir);
      return new Configuration();
    }
    LOG.info("Load flink configuration from path: {}.", flinkConfDir);
    return GlobalConfiguration.loadConfiguration(flinkConfDir.toString());
  }
}

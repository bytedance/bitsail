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

package com.bytedance.bitsail.base.version;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created 2021/8/6
 */
public class VersionHolder {
  public static final VersionHolder INSTANCE = new VersionHolder();
  private static final Logger LOG = LoggerFactory.getLogger(VersionHolder.class);
  private static final String UNKNOWN = "<unknown>";
  private static final String UNKNOWN_COMMIT_ID = "DecafC0ffeeD0d0F00d";
  private static final String PROP_FILE = ".dataleap.bitsail.version.properties";

  private String gitCommitId = UNKNOWN_COMMIT_ID;
  private String gitBuildVersion = UNKNOWN;
  private String gitBuildTime = UNKNOWN;

  private VersionHolder() {
    ClassLoader classLoader = VersionHolder.class.getClassLoader();
    try (InputStream versionFile = classLoader.getResourceAsStream(PROP_FILE)) {
      if (versionFile != null) {
        Properties properties = new Properties();
        properties.load(versionFile);

        gitCommitId = getProperty(properties, "git.commit.id", UNKNOWN_COMMIT_ID);
        gitBuildVersion = getProperty(properties, "git.build.version", UNKNOWN);
        gitBuildTime = getProperty(properties, "git.build.time", UNKNOWN);
      }
    } catch (Exception e) {
      LOG.info("Cannot determine code revision: Unable to read version property file.", e);
    }

    // Obtain git version from package info
    if (!isBuildVersionValid(gitBuildVersion)) {
      try {
        Package curPkg = VersionHolder.class.getPackage();
        VersionInfoAnnotation annotation = curPkg.getAnnotation(VersionInfoAnnotation.class);
        if (StringUtils.isNotEmpty(annotation.version())) {
          gitBuildVersion = annotation.version();
        }
      } catch (Exception ignored) {
        gitBuildVersion = UNKNOWN;
      }
    }
  }

  private static String getProperty(Properties properties, String key, String defaultValue) {
    String value = properties.getProperty(key);
    if (value == null || value.charAt(0) == '$') {
      return defaultValue;
    }
    return value;
  }

  public static VersionHolder getHolder() {
    return INSTANCE;
  }

  public static boolean isCommitIdValid(String commitId) {
    return !UNKNOWN_COMMIT_ID.equals(commitId);
  }

  public static boolean isBuildVersionValid(String buildVersion) {
    return !UNKNOWN.equals(buildVersion);
  }

  public String getGitCommitId() {
    return gitCommitId;
  }

  public String getBuildVersion() {
    return gitBuildVersion;
  }

  public String getGitBuildTime() {
    return gitBuildTime;
  }

  public static void print() {
    LOG.info("BitSail Build Version: {}.", VersionHolder.getHolder().getBuildVersion());
    LOG.info("BitSail Build Time: {}.", VersionHolder.getHolder().getGitBuildTime());
    LOG.info("BitSail Build Commit: {}.", VersionHolder.getHolder().getGitCommitId());
  }
}

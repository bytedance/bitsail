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

package com.bytedance.bitsail.test.e2e.executor;

import com.bytedance.bitsail.test.e2e.executor.generic.GenericExecutor;
import com.bytedance.bitsail.test.e2e.executor.generic.GenericExecutorSetting;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ExecutorLoader {
  private static final Logger LOG = LoggerFactory.getLogger(ExecutorLoader.class);

  private final List<String> includePatterns;
  private final List<String> excludePatterns;

  /**
   * Load those executors matches patterns.
   */
  public List<AbstractExecutor> loadAll() {
    List<AbstractExecutor> loadedExecutors = new ArrayList<>();

    // load executors
    ServiceLoader<AbstractExecutor> loader = ServiceLoader.load(AbstractExecutor.class);
    for (AbstractExecutor executor : loader) {
      if (shouldLoad(executor.getContainerName())) {
        loadedExecutors.add(executor);
        LOG.info("Load executor: {}", executor.getContainerName());
      }
    }

    // create GenericExecutors
    File module = Paths.get(AbstractExecutor.getLocalRootDir(),
        "bitsail-test",
        "bitsail-test-end-to-end",
        "bitsail-test-e2e-generic-executor-templates").toFile();
    File settingFolder = Paths.get(module.getAbsolutePath(),
        "src", "main", "settings").toFile();
    List<GenericExecutorSetting> settings = loadSettings(settingFolder);
    settings.forEach(setting -> {
      loadedExecutors.add(new GenericExecutor(setting));
      LOG.info("Load generic executor: {}", setting.getExecutorName());
    });

    return loadedExecutors;
  }

  /**
   * Load setting files from specific folder.
   */
  private List<GenericExecutorSetting> loadSettings(File settingFolder) {
    if (!settingFolder.exists() || !settingFolder.isDirectory()) {
      return Lists.newArrayList();
    }

    File[] settingFiles = settingFolder.listFiles();
    if (settingFiles == null) {
      return Lists.newArrayList();
    }

    return Arrays.stream(settingFiles)
        .filter(File::exists)
        .filter(File::isFile)
        .map(settingFile -> {
          try {
            return GenericExecutorSetting.initFromFile(settingFile.getAbsolutePath());
          } catch (Exception ignored) {
            return null;
          }
        })
        .filter(Objects::nonNull)
        .filter(setting -> shouldLoad(setting.getExecutorName()))
        .collect(Collectors.toList());
  }

  /**
   * Check if the executorName should be loaded.
   *
   * @param executorName Target executor name.
   * @return True if it should be loaded.
   */
  public boolean shouldLoad(String executorName) {
    if (CollectionUtils.isNotEmpty(includePatterns)) {
      boolean found = false;
      for (String includePattern : includePatterns) {
        Matcher matcher = Pattern.compile(includePattern).matcher(executorName);
        if (matcher.find()) {
          found = true;
          break;
        }
      }
      // executor name should match one of the `include pattern`
      if (!found) {
        return false;
      }
    }

    if (CollectionUtils.isNotEmpty(excludePatterns)) {
      for (String excludePattern : excludePatterns) {
        Matcher matcher = Pattern.compile(excludePattern).matcher(executorName);
        // executor name shouldn't match any of the `exclude pattern`
        if (matcher.find()) {
          return false;
        }
      }
    }

    return true;
  }
}

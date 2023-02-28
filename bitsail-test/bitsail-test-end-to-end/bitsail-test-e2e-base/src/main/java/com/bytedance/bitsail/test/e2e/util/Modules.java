/*
 *       Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 *       Licensed under the Apache License, Version 2.0 (the "License");
 *       you may not use this file except in compliance with the License.
 *       You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *       Unless required by applicable law or agreed to in writing, software
 *       distributed under the License is distributed on an "AS IS" BASIS,
 *       WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *       See the License for the specific language governing permissions and
 *       limitations under the License.
 */

package com.bytedance.bitsail.test.e2e.util;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;

public class Modules {

  private static final String BITSAIL_DEFAULT_PREFIX = "bitsail";
  private static final String JAR = "jar";

  public static Path getModuleResourceDir(String root, String module) {
    return Paths.get(root)
        .resolve(module)
        .resolve("src")
        .resolve("main")
        .resolve("resources");

  }

  public static Optional<File[]> getModuleResourceMappings(String root, String module) {
    Path moduleResourceDir = getModuleResourceDir(root, module);
    if (!Files.exists(moduleResourceDir)) {
      return Optional.empty();
    }
    List<File> files = Lists.newArrayList();
    for (File child : moduleResourceDir.toFile().listFiles()) {
      if (StringUtils.endsWith(child.getName(), "json")) {
        files.add(child);
      }
    }
    return Optional.of(files.toArray(new File[] {}));
  }

  public static Path getModuleTargetDir(String root, String module) {
    return Paths.get(root)
        .resolve(module)
        .resolve("target");
  }

  public static Optional<File> getModuleTargetJar(String root, String module) {
    Path moduleTargetDir = getModuleTargetDir(root, module);
    if (!Files.exists(moduleTargetDir)) {
      return Optional.empty();
    }

    for (File child : moduleTargetDir.toFile().listFiles()) {
      if (StringUtils.startsWithIgnoreCase(child.getName(), BITSAIL_DEFAULT_PREFIX)
          && StringUtils.endsWithIgnoreCase(child.getName(), JAR)) {
        return Optional.of(child);
      }
    }
    return Optional.empty();
  }
}

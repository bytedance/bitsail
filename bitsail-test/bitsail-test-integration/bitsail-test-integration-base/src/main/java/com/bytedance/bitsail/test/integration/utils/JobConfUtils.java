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

package com.bytedance.bitsail.test.integration.utils;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

public class JobConfUtils {
  public static BitSailConfiguration fromClasspath(String name) throws URISyntaxException, IOException {
    return BitSailConfiguration.from(fromClasspathToString(name));
  }

  public static String fromClasspathToString(String name) throws URISyntaxException, IOException {
    ClassLoader classLoader = JobConfUtils.class.getClassLoader();
    URL resource = classLoader.getResource(name);
    if (Objects.isNull(resource)) {
      throw new IllegalArgumentException(String.format("Resources name: %s not found in classpath.",
          name));
    }
    return new String(Files.readAllBytes(Paths.get(resource.toURI())));
  }
}

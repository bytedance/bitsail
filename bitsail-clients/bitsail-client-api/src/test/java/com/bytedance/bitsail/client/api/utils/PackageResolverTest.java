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

package com.bytedance.bitsail.client.api.utils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.URISyntaxException;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({System.class, PackageResolver.class})
public class PackageResolverTest {

  @Test
  public void getLibraryDirTest() throws URISyntaxException {
    String libraryDir = PackageResolver.getLibraryDir().toString();
    String expectLibDir = Paths.get(PackageResolverTest.class.getProtectionDomain().getCodeSource()
        .getLocation().toURI()).getParent().getParent().resolve("./libs").toString();
    assertEquals(libraryDir, expectLibDir);

    String bitSailHome = Paths.get("/test/").toString();
    PowerMockito.mockStatic(System.class);
    PowerMockito.when(System.getenv(PackageResolver.BITSAIL_HOME_KEY)).thenReturn(bitSailHome);
    libraryDir = PackageResolver.getLibraryDir().toString();
    assertEquals(libraryDir, Paths.get(bitSailHome).resolve("./libs").toString());
  }
}

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

package com.bytedance.bitsail.entry.flink.handlers;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.component.format.security.kerberos.option.KerberosOptions;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.FLINK_CONF_FILE;
import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.FLINK_LOG_FILE_PREFIX;
import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.getFlinkConfDir;
import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.loadFlinkConfiguration;

public class CustomFlinkPackageHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CustomFlinkPackageHandler.class);

  private static final String CONTEXT_CLIENT = "Client";

  /**
   * Build a custom Flink Package and return the parent directory path of such package.
   * @param sysConfiguration BitSail system configuration
   * @param flinkRunCommandArgs Flink run command arguments
   * @param originalFlinkDir Path of the original Flink directory
   * @return Path of the parent directory of new created custom Flink package.
   * @throws IOException IOException
   */
  public static Path buildCustomFlinkPackage(BitSailConfiguration sysConfiguration,
                                             FlinkCommandArgs flinkRunCommandArgs,
                                             Path originalFlinkDir) throws IOException {
    Configuration flinkConfiguration = loadFlinkConfiguration(getFlinkConfDir(originalFlinkDir));
    processSecurity(sysConfiguration, flinkConfiguration);
    Path tmpFlinkConfDir = writeConfToTmpFile(flinkConfiguration);
    symbolicLinkFlinkLog4j(originalFlinkDir, tmpFlinkConfDir);
    return tmpFlinkConfDir;
  }

  static void processSecurity(BitSailConfiguration sysConfiguration, Configuration flinkConfiguration) {
    if (!sysConfiguration.get(KerberosOptions.KERBEROS_ENABLE)) {
      return;
    }
    LOG.info("Kerberos (global) is enabled.");
    String principal = sysConfiguration.get(KerberosOptions.KERBEROS_PRINCIPAL);
    String keytabPath = sysConfiguration.get(KerberosOptions.KERBEROS_KEYTAB_PATH);
    String krb5Path = sysConfiguration.get(KerberosOptions.KERBEROS_KRB5_CONF_PATH);

    flinkConfiguration.set(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
    flinkConfiguration.set(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, principal);
    flinkConfiguration.setString("security.kerberos.krb5-conf.path", krb5Path);

    String loginContexts = flinkConfiguration.get(SecurityOptions.KERBEROS_LOGIN_CONTEXTS);
    if (StringUtils.isNotEmpty(loginContexts)) {
      Set<String> contextSet = Arrays.stream(loginContexts.split(",")).collect(Collectors.toSet());
      contextSet.add(CONTEXT_CLIENT);
      loginContexts = contextSet.stream().collect(Collectors.joining(","));
      flinkConfiguration.set(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, loginContexts);
    } else {
      flinkConfiguration.set(SecurityOptions.KERBEROS_LOGIN_CONTEXTS, CONTEXT_CLIENT);
    }
  }

  static Path writeConfToTmpFile(Configuration flinkConfiguration) throws IOException {
    File tmpDir = Files.createTempDir();

    File tmpFlinkConf = Paths.get(tmpDir.getPath(),
        UUID.randomUUID().toString(),
        FLINK_CONF_FILE).toFile();

    if (tmpFlinkConf.exists()) {
      FileUtils.deleteQuietly(tmpFlinkConf);
    }
    LOG.info("Creating new tmp flink-conf file in path {}", tmpFlinkConf.toPath());
    Path tmpFlinkConfDir = Paths.get(tmpFlinkConf.getParent());
    Files.createParentDirs(tmpFlinkConf);
    //register delete on exit.
    tmpFlinkConfDir.toFile().deleteOnExit();
    tmpFlinkConf.createNewFile();
    tmpFlinkConf.deleteOnExit();

    try (FileWriter fileWriter = new FileWriter(tmpFlinkConf);
         PrintWriter printWriter = new PrintWriter(fileWriter)) {
      for (String key : flinkConfiguration.keySet()) {
        String value = flinkConfiguration.getString(key, null);
        printWriter.print(key);
        printWriter.print(": ");
        printWriter.println(value);
      }
      LOG.info("Success to write flink conf to file.");
      return tmpFlinkConfDir;
    }
  }

  /**
   * Flink will find log4j/logback configuration in the ENV property `FLINK_CONF_DIR`.
   * So if we want to change the flink conf dir to temporary dir, we need to link the log configuration files
   * to the temporary dir as well.
   */
  private static void symbolicLinkFlinkLog4j(Path flinkDir, Path tmpFlinkConfDir) throws IOException {
    Path flinkConfDir = getFlinkConfDir(flinkDir);
    try (Stream<Path> flinkLogConfPath = java.nio.file.Files.list(flinkConfDir)) {
      List<Path> flinkLogConfPaths = flinkLogConfPath.filter(file -> file.getFileName().toString()
              .startsWith(FLINK_LOG_FILE_PREFIX))
          .collect(Collectors.toList());
      for (Path flinkLogConf : flinkLogConfPaths) {
        Path resolve = tmpFlinkConfDir.resolve(flinkLogConf.getFileName());
        LOG.info("Create flink log symbolic link from {} to {}.", flinkLogConf, resolve);
        java.nio.file.Files.createSymbolicLink(resolve, flinkLogConf);
        resolve.toFile().deleteOnExit();
      }
    }
  }
}

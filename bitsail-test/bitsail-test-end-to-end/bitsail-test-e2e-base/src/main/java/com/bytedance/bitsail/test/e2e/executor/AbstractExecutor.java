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

import com.bytedance.bitsail.base.version.VersionHolder;
import com.bytedance.bitsail.common.BitSailException;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.base.AbstractContainer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.error.E2ETestErrorCode;
import com.bytedance.bitsail.test.e2e.mapping.ConnectorMapping;

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Executor for running BitSail job.
 */
@Data
public abstract class AbstractExecutor extends AbstractContainer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractExecutor.class);

  public static final String BITSAIL_REVISION = "bitsail.revision";
  public static final String BITSAIL_ROOT_DIR = "bitsail.rootDir";
  public static final String BITSAIL_E2E_EXECUTOR_ROOT_DIR = "/opt/bitsail";

  protected static final String BIN_PATH = Paths.get(
      "bitsail-dist", "src", "main", "archive", "bin", "bitsail").toString();
  protected static final String LOGBACK_FILE = Paths.get(
      "bitsail-dist", "src", "main", "resources", "logback.xml").toString();
  protected static final String SYS_CONF_PATH = Paths.get(
      "bitsail-test",
      "bitsail-test-end-to-end",
      "bitsail-test-e2e-base",
      "src", "main", "resources", "conf", "bitsail.conf"
  ).toString();
  protected static final String CORE_LIB_PATH = Paths.get(
      "bitsail-cores", "bitsail-core-entry", "target").toString();
  protected static final String CLIENT_LIB_PATH = Paths.get(
      "bitsail-clients", "bitsail-client-entry", "target").toString();

  /**
   * Core modules of executor.
   */
  protected List<String> coreModules;

  /**
   * Client modules of executor.
   */
  protected List<String> clientModules;

  /**
   * Configuration of executor and job.
   */
  protected BitSailConfiguration executorConf;

  /**
   * Revision of bitsail libs.
   */
  protected String revision;

  /**
   * Files to copy from local resource to executor, including jar files and job configurations.
   */
  protected Set<TransferableFile> transferableFiles;

  /**
   * Root dir of the project in executor when running e2e test.
   */
  protected String executorRootDir;

  /**
   * Local project dir.
   */
  @Getter
  protected static String localRootDir;

  static {
    // init localRootDir
    File classFile = new File(AbstractExecutor.class.getProtectionDomain().getCodeSource().getLocation().getPath());
    Preconditions.checkNotNull(classFile);

    File curDir = classFile;
    while (!"bitsail-test".equals(curDir.getName())) {
      curDir = curDir.getParentFile();
    }
    localRootDir = curDir.getParentFile().getAbsolutePath();

    Preconditions.checkState(new File(localRootDir).exists(), "Failed to locate project.");
    LOG.info("Detect project root dir: {}", localRootDir);
  }

  @Override
  public void initNetwork(Network network) {
    this.network = network != null ? network : Network.newNetwork();
  }

  /**
   * Configure the executor before initialization.
   */
  public void configure(BitSailConfiguration executorConf) {
    this.executorConf = executorConf;
    this.revision = VersionHolder.getHolder().getBuildVersion();
    this.transferableFiles = new HashSet<>();
    this.executorRootDir = BITSAIL_E2E_EXECUTOR_ROOT_DIR;

    try {
      addBitsailLibs();
    } catch (URISyntaxException e) {
      throw new RuntimeException("Failed to load bitsail libs into executor.", e);
    }

    try {
      addConnectorLibs();
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Failed to load connector libs.", e);
    }

    addJobConf(executorConf);
  }

  /**
   * Initialize the executor.
   */
  public abstract void init();

  /**
   * Run BitSail job in the executor.
   * @param testId Identification of current test case.
   * @return Exit code of bitsail program.
   */
  public abstract int run(String testId) throws Exception;

  /**
   * Add engine and corresponding client entry into transferable files.
   */
  protected void addEngineLibs(String buildVersion) {
    if (CollectionUtils.isNotEmpty(coreModules)) {
      coreModules.forEach(this::addCoreModuleLibs);
    }

    if (CollectionUtils.isNotEmpty(clientModules)) {
      clientModules.forEach(this::addClientModuleLibs);
    }
  }

  /**
   * Add connector libs into transferable files.
   */
  protected void addConnectorLibs() throws FileNotFoundException {
    String readerClass = executorConf.get(ReaderOptions.READER_CLASS);
    String writerClass = executorConf.get(WriterOptions.WRITER_CLASS);

    List<ConnectorMapping.ConnectorLib> connectorLibs = Lists.newArrayList(
        ConnectorMapping.getInstance().getLibByClassName(readerClass),
        ConnectorMapping.getInstance().getLibByClassName(writerClass)
    );

    for (ConnectorMapping.ConnectorLib connectorLib : connectorLibs) {
      // libs/connectors/mapping/
      String mappingFile = connectorLib.getMappingFile();
      String mappingFileName = new File(mappingFile).getName();
      transferableFiles.add(new TransferableFile(mappingFile,
          Paths.get(executorRootDir, "libs", "connectors", "mapping", mappingFileName).toAbsolutePath().toString()));

      // libs/connectors/
      List<String> libFiles = connectorLib.getLibFiles();
      for (String libFile : libFiles) {
        String libFileName = new File(libFile).getName();
        transferableFiles.add(new TransferableFile(libFile,
            Paths.get(executorRootDir, "libs", "connectors", libFileName).toAbsolutePath().toString()));
      }

      LOG.info("Successfully add libs for connector: [{}].", connectorLib.getName());
    }
  }

  /**
   * List bitsail libs and binary files.
   */
  protected void addBitsailLibs() throws URISyntaxException {
    String buildVersion = VersionHolder.getHolder().getBuildVersion();

    // bin/bitsail
    String bin = Paths.get(localRootDir, BIN_PATH)
        .toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(bin,
        Paths.get(executorRootDir, "bin", "bitsail").toAbsolutePath().toString()));

    // conf/logback.xml
    String logback = Paths.get(localRootDir, LOGBACK_FILE)
        .toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(logback,
        Paths.get(executorRootDir, "conf", "logback.xml").toAbsolutePath().toString()));

    // conf/bitsail.conf
    String sysConf = Paths.get(localRootDir, SYS_CONF_PATH).toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(sysConf,
        Paths.get(executorRootDir, "conf", "bitsail.conf").toAbsolutePath().toString()));

    // libs/bitsail-core.jar
    String coreJar = Paths.get(localRootDir, CORE_LIB_PATH,
        "bitsail-core-entry-" + buildVersion + ".jar").toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(coreJar,
        Paths.get(executorRootDir, "libs", "bitsail-core.jar").toAbsolutePath().toString()));

    // libs/clients/bitsail-client-entry-{revision}.jar
    String clientEntryFile = "bitsail-client-entry-" + buildVersion + ".jar";
    String clientEntry = Paths.get(localRootDir, CLIENT_LIB_PATH, clientEntryFile)
        .toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(clientEntry,
        Paths.get(executorRootDir, "libs", "clients", clientEntryFile).toAbsolutePath().toString()));

    LOG.info("Successfully load bitsail libs.");

    // libs/engines
    // libs/clients/bitsail-client-entry-{engine}-{revision}.jar
    addEngineLibs(buildVersion);
  }

  /**
   * Add job conf file.
   */
  protected void addJobConf(BitSailConfiguration executorConf) {
    BitSailConfiguration jobConf = executorConf.clone();
    Path libPath = Paths.get(executorRootDir, "libs").toAbsolutePath();
    jobConf.set(CommonOptions.JOB_PLUGIN_ROOT_PATH, libPath.toString());

    File tmpJobConf;
    try {
      tmpJobConf = File.createTempFile("jobConf", ".json");
      tmpJobConf.deleteOnExit();
      BufferedWriter out = new BufferedWriter(new FileWriter(tmpJobConf));
      out.write(jobConf.toJSON());
      out.newLine();
      out.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create tmp job conf file.");
    }

    transferableFiles.add(new TransferableFile(tmpJobConf.getAbsolutePath(),
        Paths.get(executorRootDir, "jobConf.json").toAbsolutePath().toString()));
  }

  /**
   * Add libs of specific core module.
   * @param moduleName Target core module name.
   */
  private void addCoreModuleLibs(String moduleName) {
    File targetFolder = Paths.get(localRootDir, moduleName, "target").toFile();
    if (!targetFolder.exists()) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_FOUND,
          "Core module " + moduleName + " not found");
    }

    File[] targetFiles = targetFolder.listFiles();
    if (targetFiles == null) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_COMPILED,
          "Cannot found libs in core module " + moduleName + ", please build project.");
    }

    List<TransferableFile> coreLibs = Arrays.stream(targetFiles)
        .filter(file -> file.getName().endsWith(".jar"))
        .filter(file -> !file.getName().startsWith("original-"))
        .map(file -> {
          String localPath = file.getAbsolutePath();
          String remotePath = Paths.get(executorRootDir,
              "libs", "engines", file.getName()).toAbsolutePath().toString();
          return new TransferableFile(localPath, remotePath);
        }).collect(Collectors.toList());
    if (CollectionUtils.isEmpty(coreLibs)) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_COMPILED,
          "Cannot found libs in core module " + moduleName + ", please build project.");
    }
    transferableFiles.addAll(coreLibs);

    File resourceFolder = Paths.get(localRootDir, moduleName, "src", "main", "resources").toFile();
    File[] confFiles = resourceFolder.listFiles();
    if (confFiles != null) {
      Arrays.stream(confFiles)
          .filter(file -> file.getName().endsWith(".json"))
          .forEach(file -> {
            String localPath = file.getAbsolutePath();
            String remotePath = Paths.get(executorRootDir,
                "libs", "engines", "mapping", file.getName()).toAbsolutePath().toString();
            transferableFiles.add(new TransferableFile(localPath, remotePath));
          });
    }

    LOG.info("Successfully add libs for core module: {}", moduleName);
  }

  /**
   * Add libs of specific client module.
   * @param moduleName Target client module name.
   */
  private void addClientModuleLibs(String moduleName) {
    File clientModule = Paths.get(localRootDir, moduleName, "target").toFile();
    if (!clientModule.exists()) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_FOUND,
          "Client module " + moduleName + " not found");
    }

    File[] targetFiles = clientModule.listFiles();
    if (targetFiles == null) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_COMPILED,
          "Cannot found libs in client module " + moduleName + ", please build project.");
    }

    List<TransferableFile> clientLibs = Arrays.stream(targetFiles)
        .filter(file -> file.getName().endsWith(".jar"))
        .map(file -> {
          String localPath = file.getAbsolutePath();
          String remotePath = Paths.get(executorRootDir,
              "libs", "clients", file.getName()).toAbsolutePath().toString();
          return new TransferableFile(localPath, remotePath);
        }).collect(Collectors.toList());
    if (CollectionUtils.isEmpty(clientLibs)) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_COMPILED,
          "Cannot found libs in client module " + moduleName + ", please build project.");
    }
    transferableFiles.addAll(clientLibs);

    LOG.info("Successfully add libs for client module: {}", moduleName);
  }
}

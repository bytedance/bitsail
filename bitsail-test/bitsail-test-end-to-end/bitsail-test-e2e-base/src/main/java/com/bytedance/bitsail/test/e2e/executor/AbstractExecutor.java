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
import com.bytedance.bitsail.test.e2e.util.Modules;

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Getter;
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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Executor for running BitSail job.
 */
@Data
public abstract class AbstractExecutor extends AbstractContainer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractExecutor.class);

  private static final String NOT_MVN_PACKAGE_MSG = "Module not package before execute e2e, plz run `mvn package -pl %s -am` first.";

  public static final String BITSAIL_REVISION = "bitsail.revision";
  public static final String BITSAIL_ROOT_DIR = "bitsail.rootDir";

  protected static final String BITSAIL_ENTRY_JAR = "bitsail-core.jar";
  protected static final Path EXECUTOR_ROOT_DIR = Paths.get("/opt/bitsail");

  private static final Path EXECUTOR_LIBRARIES_DIR = EXECUTOR_ROOT_DIR.resolve("libs");
  private static final Path EXECUTOR_CLIENTS_DIR = EXECUTOR_LIBRARIES_DIR.resolve("clients");
  private static final Path EXECUTOR_CLIENTS_ENGINE_MAPPING_DIR = EXECUTOR_CLIENTS_DIR.resolve("mapping");
  private static final Path EXECUTOR_CLIENTS_ENGINE_DIR = EXECUTOR_CLIENTS_DIR.resolve("engines");
  private static final Path EXECUTOR_ENGINES_DIR = EXECUTOR_LIBRARIES_DIR.resolve("engines");
  private static final Path EXECUTOR_ENGINES_MAPPING_DIR = EXECUTOR_ENGINES_DIR.resolve("mapping");
  private static final Path EXECUTOR_CONNECTORS_DIR = EXECUTOR_LIBRARIES_DIR.resolve("connectors");
  private static final Path EXECUTOR_CONNECTOR_MAPPING_DIR = EXECUTOR_CONNECTORS_DIR.resolve("mapping");

  private static final String BITSAIL_DIST_MODULE = "bitsail-dist";
  private static final String BITSAIL_CLIENT_ENTRY_MODULE = "bitsail-clients" + File.separator + "bitsail-client-entry";
  private static final String BITSAIL_CORE_ENTRY_MODULE = "bitsail-cores" + File.separator + "bitsail-core-entry";

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

    try {
      addFrameworkLibraries();
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
   *
   * @param testId Identification of current test case.
   * @return Exit code of bitsail program.
   */
  public abstract int run(String testId) throws Exception;

  public abstract String getClientEngineModule();

  public abstract String getCoreEngineModule();

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
          EXECUTOR_CONNECTOR_MAPPING_DIR.resolve(mappingFileName).toAbsolutePath().toString()));

      // libs/connectors/
      List<String> libFiles = connectorLib.getLibFiles();
      for (String libFile : libFiles) {
        String libFileName = new File(libFile).getName();
        transferableFiles.add(new TransferableFile(libFile,
            EXECUTOR_CONNECTORS_DIR.resolve(libFileName).toAbsolutePath().toString()));
      }

      LOG.info("Successfully add libs for connector: [{}].", connectorLib.getName());
    }
  }

  /**
   * List bitsail libs and binary files.
   */
  protected void addFrameworkLibraries() throws URISyntaxException {
    //root dir.
    Path rootDir = Modules.getModuleResourceDir(localRootDir, BITSAIL_DIST_MODULE);
    transferableFiles.add(new TransferableFile(rootDir.toAbsolutePath().toString(), EXECUTOR_ROOT_DIR.toAbsolutePath().toString()));

    // libs/bitsail-core.jar
    Optional<File> coreEntryModule = Modules.getModuleTargetJar(localRootDir, BITSAIL_CORE_ENTRY_MODULE);
    if (!coreEntryModule.isPresent()) {
      throw BitSailException.asBitSailException(E2ETestErrorCode.MODULE_NOT_COMPILED, String.format(NOT_MVN_PACKAGE_MSG, BITSAIL_CORE_ENTRY_MODULE));
    }
    transferableFiles.add(new TransferableFile(coreEntryModule.get().getAbsolutePath(),
        EXECUTOR_LIBRARIES_DIR.resolve(BITSAIL_ENTRY_JAR).toAbsolutePath().toString()));

    // libs/clients/bitsail-client-entry-{revision}.jar
    Optional<File> clientEntryModule = Modules.getModuleTargetJar(localRootDir, BITSAIL_CLIENT_ENTRY_MODULE);

    if (!clientEntryModule.isPresent()) {
      throw BitSailException.asBitSailException(E2ETestErrorCode.MODULE_NOT_COMPILED, String.format(NOT_MVN_PACKAGE_MSG, BITSAIL_CLIENT_ENTRY_MODULE));
    }

    File clientEntryModuleJar = clientEntryModule.get();
    transferableFiles.add(new TransferableFile(clientEntryModuleJar.getAbsolutePath(),
        EXECUTOR_CLIENTS_DIR.resolve(clientEntryModuleJar.getName()).toAbsolutePath().toString()));

    LOG.info("Successfully load bitsail framework libs.");

    // libs/engines
    // libs/clients/bitsail-client-entry-{engine}-{revision}.jar
    addEngineModuleLibrary(getCoreEngineModule(), EXECUTOR_ENGINES_DIR, EXECUTOR_ENGINES_MAPPING_DIR);
    addEngineModuleLibrary(getClientEngineModule(), EXECUTOR_CLIENTS_ENGINE_DIR, EXECUTOR_CLIENTS_ENGINE_MAPPING_DIR);
  }

  /**
   * Add library of specific module.
   *
   * @param moduleName Target core module name.
   */
  private void addEngineModuleLibrary(String moduleName,
                                      Path moduleExecutorPath,
                                      Path moduleMappingExecutorPath) {
    Optional<File> optional = Modules.getModuleTargetJar(localRootDir, moduleName);
    if (!optional.isPresent()) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_COMPILED,
          String.format(NOT_MVN_PACKAGE_MSG, moduleName));
    }

    File moduleFile = optional.get();
    transferableFiles.add(new TransferableFile(moduleFile.getAbsolutePath(), moduleExecutorPath.resolve(moduleFile.getName()).toAbsolutePath().toString()));
    Optional<File[]> moduleResourceMappings = Modules.getModuleResourceMappings(localRootDir, moduleName);
    if (!moduleResourceMappings.isPresent()) {
      throw BitSailException.asBitSailException(
          E2ETestErrorCode.MODULE_NOT_FOUND,
          String.format("Module %s's mapping file not exists.", moduleName));
    }
    for (File child : moduleResourceMappings.get()) {
      transferableFiles.add(new TransferableFile(child.getAbsolutePath(), moduleMappingExecutorPath.resolve(child.getName()).toAbsolutePath().toString()));
    }

    LOG.info("Successfully add library for module: {}", moduleName);
  }

  /**
   * Add job conf file.
   */
  protected void addJobConf(BitSailConfiguration executorConf) {
    BitSailConfiguration jobConf = executorConf.clone();
    jobConf.set(CommonOptions.JOB_PLUGIN_ROOT_PATH, EXECUTOR_LIBRARIES_DIR.toAbsolutePath().toString());

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
        EXECUTOR_ROOT_DIR.resolve("jobConf.json").toAbsolutePath().toString()));
  }
}

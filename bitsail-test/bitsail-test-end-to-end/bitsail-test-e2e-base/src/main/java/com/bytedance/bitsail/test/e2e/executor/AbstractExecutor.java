/*
 * Copyright 2022 Bytedance Ltd. and/or its affiliates.
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
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.option.CommonOptions;
import com.bytedance.bitsail.common.option.ReaderOptions;
import com.bytedance.bitsail.common.option.WriterOptions;
import com.bytedance.bitsail.common.util.Preconditions;
import com.bytedance.bitsail.test.e2e.base.AbstractContainer;
import com.bytedance.bitsail.test.e2e.base.transfer.TransferableFile;
import com.bytedance.bitsail.test.e2e.mapping.ConnectorMapping;

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.Set;

/**
 * Executor for running BitSail job.
 */
@Data
public abstract class AbstractExecutor extends AbstractContainer {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractExecutor.class);

  public static final String BITSAIL_REVISION = "bitsail.revision";
  public static final String BITSAIL_ROOT_DIR = "bitsail.rootDir";
  public static final String BITSAIL_E2E_EXECUTOR_ROOT_DIR = "/opt/bitsail";

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
  protected abstract void addEngineLibs();

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
    String bin = Paths.get(localRootDir,
        "bitsail-dist", "src", "main", "archive", "bin", "bitsail")
        .toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(bin,
        Paths.get(executorRootDir, "bin", "bitsail").toAbsolutePath().toString()));

    // conf/logback.xml
    String logback = Paths.get(localRootDir,
        "bitsail-dist", "src", "main", "resources", "logback.xml")
        .toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(logback,
        Paths.get(executorRootDir, "conf", "logback.xml").toAbsolutePath().toString()));

    // conf/bitsail.conf
    String sysConf = Paths.get(localRootDir,
        "bitsail-test",
        "bitsail-test-end-to-end",
        "bitsail-test-e2e-base",
        "src", "main", "resources", "conf", "bitsail.conf").toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(sysConf,
        Paths.get(executorRootDir, "conf", "bitsail.conf").toAbsolutePath().toString()));

    // libs/bitsail-core.jar
    String coreJar = Paths.get(localRootDir,
        "bitsail-cores",
        "bitsail-core-entry",
        "target", "bitsail-core-entry-" + buildVersion + ".jar").toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(coreJar,
        Paths.get(executorRootDir, "libs", "bitsail-core.jar").toAbsolutePath().toString()));

    // libs/clients/bitsail-client-entry-{revision}.jar
    String clientEntryFile = "bitsail-client-entry-" + buildVersion + ".jar";
    String clientEntry = Paths.get(localRootDir,
        "bitsail-clients",
        "bitsail-client-entry",
        "target", clientEntryFile).toAbsolutePath().toString();
    transferableFiles.add(new TransferableFile(clientEntry,
        Paths.get(executorRootDir, "libs", "clients", clientEntryFile).toAbsolutePath().toString()));

    // log libs under libs/clients
    Path logLibPath = Paths.get(localRootDir,
        "bitsail-test",
        "bitsail-test-end-to-end",
        "bitsail-test-e2e-base",
        "target", "log-libs");
    File[] logLibs = logLibPath.toFile().listFiles();
    Preconditions.checkNotNull(logLibs, "Cannot find log libs from {}", logLibPath);
    for (File logLib : logLibs) {
      String logLibFile = logLib.getName();
      transferableFiles.add(new TransferableFile(logLibPath.resolve(logLibFile).toAbsolutePath().toString(),
          Paths.get(executorRootDir, "libs", "clients", logLibFile).toAbsolutePath().toString()));
    }
    LOG.info("Successfully load bitsail libs.");

    // libs/engines
    // libs/clients/bitsail-client-entry-{engine}-{revision}.jar
    addEngineLibs();
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
}

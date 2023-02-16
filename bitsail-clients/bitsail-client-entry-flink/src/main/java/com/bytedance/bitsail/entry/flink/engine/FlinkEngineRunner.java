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

package com.bytedance.bitsail.entry.flink.engine;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.client.api.command.CommandAction;
import com.bytedance.bitsail.client.api.command.CommandArgsParser;
import com.bytedance.bitsail.client.api.engine.EngineRunner;
import com.bytedance.bitsail.client.api.utils.PackageResolver;
import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.common.configuration.ConfigParser;
import com.bytedance.bitsail.common.exception.CommonErrorCode;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;
import com.bytedance.bitsail.entry.flink.configuration.FlinkRunnerConfigOptions;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplierFactory;
import com.bytedance.bitsail.entry.flink.handlers.CustomFlinkPackageHandler;
import com.bytedance.bitsail.entry.flink.savepoint.FlinkRunnerSavepointLoader;
import com.bytedance.bitsail.entry.flink.utils.FlinkDirectory;
import com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver;

import com.google.common.collect.Lists;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplierFactory.DEPLOYMENT_KUBERNETES_APPLICATION;
import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.ENV_PROP_FLINK_CONF_DIR;

/**
 * Created 2022/8/5
 */
public class FlinkEngineRunner implements EngineRunner {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkEngineRunner.class);

  private static final String ENTRY_JAR_NAME = "bitsail-core.jar";

  private DeploymentSupplierFactory deploymentSupplierFactory;

  private BitSailConfiguration sysConfiguration;

  private Path flinkDir;

  @Override
  public void initializeEngine(BitSailConfiguration sysConfiguration) {
    this.deploymentSupplierFactory = new DeploymentSupplierFactory();
    this.sysConfiguration = sysConfiguration;
    this.flinkDir = Paths.get(sysConfiguration.getNecessaryOption(FlinkRunnerConfigOptions.FLINK_HOME, CommonErrorCode.CONFIG_ERROR));
    LOG.info("Find flink dir = {} in System configuration.", flinkDir);
    if (!Files.exists(flinkDir)) {
      LOG.error("Flink dir = {} not exists in fact, plz check the system configuration.", flinkDir);
      throw new IllegalArgumentException(String.format("Flink dir %s not exists.", flinkDir));
    }
  }

  @Override
  @SneakyThrows
  public void loadLibrary(URLClassLoader classLoader) {
    Path flinkLibDir = FlinkPackageResolver.getFlinkDir(flinkDir, FlinkDirectory.LIB);
    LOG.info("Load flink library from path: {}.", flinkLibDir);

    try (Stream<Path> libraries = Files.list(flinkLibDir)) {
      List<Path> flinkRuntimeLibraries = libraries
          .filter(library -> StringUtils.startsWith(library.getFileName().toString(), FlinkPackageResolver.FLINK_LIB_DIST_JAR_NAME))
          .collect(Collectors.toList());
      Method method = URLClassLoader.class.getDeclaredMethod("addURL", URL.class);
      method.setAccessible(true);
      for (Path runtimeLibrary : flinkRuntimeLibraries) {
        method.invoke(classLoader, runtimeLibrary.toFile().toURL());
        LOG.info("Load flink runtime library {} to classpath.", runtimeLibrary);
      }
    }
  }

  @Override
  public ProcessBuilder getProcBuilder(BaseCommandArgs baseCommandArgs) throws IOException {
    String argsMainAction = baseCommandArgs.getMainAction();

    switch (argsMainAction) {
      case CommandAction.RUN_COMMAND:
        return getRunProcBuilder(baseCommandArgs);
      case CommandAction.STOP_COMMAND:
        return getStopProcBuilder(baseCommandArgs);
      default:
        throw new UnsupportedOperationException(String.format("Main action %s not support in flink engine.", argsMainAction));
    }
  }

  private ProcessBuilder getRunProcBuilder(BaseCommandArgs baseCommandArgs) throws IOException {
    FlinkCommandArgs flinkRunCommandArgs = new FlinkCommandArgs();
    CommandArgsParser.parseArguments(baseCommandArgs.getUnknownOptions(), flinkRunCommandArgs);
    ProcessBuilder flinkProcBuilder = new ProcessBuilder();
    flinkProcBuilderBuildWithRunCommands(flinkProcBuilder, baseCommandArgs, flinkRunCommandArgs);
    flinkProcBuilderBuildWithEnvProperties(flinkProcBuilder, flinkRunCommandArgs);
    return flinkProcBuilder;
  }

  private ProcessBuilder getStopProcBuilder(BaseCommandArgs baseCommandArgs) {
    FlinkCommandArgs flinkStopCommandArgs = new FlinkCommandArgs();
    CommandArgsParser.parseArguments(baseCommandArgs.getUnknownOptions(), flinkStopCommandArgs);
    ProcessBuilder flinkProcBuilder = new ProcessBuilder();
    flinkProcBuilderWithStopCommands(flinkProcBuilder, baseCommandArgs, flinkStopCommandArgs);
    return flinkProcBuilder;
  }

  private void flinkProcBuilderBuildWithRunCommands(ProcessBuilder flinkProcBuilder, BaseCommandArgs baseCommandArgs, FlinkCommandArgs flinkRunCommandArgs) {
    BitSailConfiguration jobConfiguration = StringUtils.isNotBlank(baseCommandArgs.getJobConf()) ?
        ConfigParser.fromRawConfPath(baseCommandArgs.getJobConf()) :
        BitSailConfiguration.from(
            new String(Base64.getDecoder().decode(baseCommandArgs.getJobConfInBase64())));
    DeploymentSupplier deploymentSupplier = deploymentSupplierFactory.getDeploymentSupplier(flinkRunCommandArgs,
        jobConfiguration);
    List<String> flinkCommands = Lists.newArrayList();

    flinkCommands.add(flinkDir + "/bin/flink");
    flinkCommands.add(flinkRunCommandArgs.getExecutionMode());
    deploymentSupplier.addDeploymentMode(flinkCommands);
    deploymentSupplier.addRunDeploymentCommands(baseCommandArgs);

    FlinkRunnerSavepointLoader.loadSavepointPath(sysConfiguration,
        jobConfiguration,
        baseCommandArgs,
        flinkRunCommandArgs,
        flinkCommands);

    if (!baseCommandArgs.isDetach()) {
      flinkCommands.add("-sae");
    } else {
      flinkCommands.add("--detached");
    }

    if (sysConfiguration.fieldExists(FlinkRunnerConfigOptions.FLINK_DEFAULT_PROPERTIES)) {
      for (Map.Entry<String, String> property : sysConfiguration.getFlattenMap(FlinkRunnerConfigOptions.FLINK_DEFAULT_PROPERTIES.key()).entrySet()) {
        LOG.info("Add System property {} = {}.", property.getKey(), property.getValue());
        flinkCommands.add("-D");
        flinkCommands.add(StringUtils.trim(property.getKey()) + "=" + StringUtils.trim((property.getValue())));
      }
    }

    for (Map.Entry<String, String> property : baseCommandArgs.getProperties().entrySet()) {
      LOG.info("Add Users property {} = {}.", property.getKey(), property.getValue());
      flinkCommands.add("-D");
      flinkCommands.add(StringUtils.trim(property.getKey()) + "=" + StringUtils.trim(property.getValue()));
    }

    /*
     * Customize path of BitSail JAR file for Kubernetes application mode, because Kubernetes
     * application mode bundles BitSail JAR file together with the custom image and runs the user
     * code's main() method on the cluster. Programmed path for
     * BitSail JAR: local:///opt/flink/usrlibs/bitsail-core.jar
     */
    if (DEPLOYMENT_KUBERNETES_APPLICATION.equalsIgnoreCase(flinkRunCommandArgs.getDeploymentMode())) {
      flinkCommands.add("local:///opt/flink/usrlibs/" + ENTRY_JAR_NAME);
    } else {
      flinkCommands.add(PackageResolver.getLibraryDir().resolve(ENTRY_JAR_NAME).toString());
    }
    if (StringUtils.isNotBlank(baseCommandArgs.getJobConf())) {
      flinkCommands.add("-xjob_conf");
      flinkCommands.add(baseCommandArgs.getJobConf());
    }
    if (StringUtils.isNotBlank(baseCommandArgs.getJobConfInBase64())) {
      flinkCommands.add("-xjob_conf_in_base64");
      flinkCommands.add(baseCommandArgs.getJobConfInBase64());
    }
    flinkProcBuilder.command(flinkCommands);
  }

  private void flinkProcBuilderBuildWithEnvProperties(ProcessBuilder flinkProcBuilder, FlinkCommandArgs flinkRunCommandArgs) throws IOException {
    Path customFlinkPackageDir = CustomFlinkPackageHandler.buildCustomFlinkPackage(sysConfiguration, flinkRunCommandArgs, flinkDir);
    if (Objects.nonNull(customFlinkPackageDir)) {
      Map<String, String> envProps = flinkProcBuilder.environment();
      envProps.put(ENV_PROP_FLINK_CONF_DIR, customFlinkPackageDir.toString());
      LOG.info("Set env prop in procBuilder: {}={}", ENV_PROP_FLINK_CONF_DIR, customFlinkPackageDir);
    }
  }

  private void flinkProcBuilderWithStopCommands(ProcessBuilder flinkProcBuilder, BaseCommandArgs baseCommandArgs, FlinkCommandArgs flinkStopCommandArgs) {
    DeploymentSupplier deploymentSupplier = deploymentSupplierFactory.getDeploymentSupplier(flinkStopCommandArgs, null);
    List<String> flinkCommands = Lists.newArrayList();

    flinkCommands.add(flinkDir + "/bin/flink");
    flinkCommands.add(flinkStopCommandArgs.getExecutionMode());
    deploymentSupplier.addDeploymentMode(flinkCommands);
    deploymentSupplier.addStopDeploymentCommands(baseCommandArgs);

    flinkCommands.add(flinkStopCommandArgs.getJobId());

    flinkProcBuilder.command(flinkCommands);
  }

  @Override
  public String engineName() {
    return "flink";
  }

}

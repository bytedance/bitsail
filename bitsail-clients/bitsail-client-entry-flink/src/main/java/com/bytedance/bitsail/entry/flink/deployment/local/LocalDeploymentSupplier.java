package com.bytedance.bitsail.entry.flink.deployment.local;

import com.bytedance.bitsail.client.api.command.BaseCommandArgs;
import com.bytedance.bitsail.entry.flink.command.FlinkRunCommandArgs;
import com.bytedance.bitsail.entry.flink.deployment.DeploymentSupplier;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LocalDeploymentSupplier implements DeploymentSupplier {
  private static final Logger LOG = LoggerFactory.getLogger(LocalDeploymentSupplier.class);

  private final FlinkRunCommandArgs flinkCommandArgs;

  public LocalDeploymentSupplier(FlinkRunCommandArgs flinkCommandArgs) {
    this.flinkCommandArgs = flinkCommandArgs;
  }

  @Override
  public void addDeploymentCommands(BaseCommandArgs baseCommandArgs, List<String> flinkCommands) {
    String jobManagerAddress = flinkCommandArgs.getJobManagerAddress();
    if (StringUtils.isNotEmpty(jobManagerAddress)) {
      flinkCommands.add("-m");
      flinkCommands.add(jobManagerAddress);
    } else {
      LOG.info("Job manager is not specified. Job will be submit to default job manager.");
    }
  }
}

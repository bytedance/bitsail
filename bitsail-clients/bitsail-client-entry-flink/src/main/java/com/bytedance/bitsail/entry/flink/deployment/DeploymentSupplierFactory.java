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

package com.bytedance.bitsail.entry.flink.deployment;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.entry.flink.command.FlinkCommandArgs;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/**
 * Created 2022/8/8
 */
public class DeploymentSupplierFactory {

  public DeploymentSupplier getDeploymentSupplier(FlinkCommandArgs flinkCommandArgs, BitSailConfiguration jobConfiguration) {
    ServiceLoader<DeploymentSupplier> loader = ServiceLoader.load(DeploymentSupplier.class);

    List<DeploymentSupplier> acceptableSupplier = new ArrayList<>();
    for (DeploymentSupplier deploymentSupplier : loader) {
      if (deploymentSupplier != null && deploymentSupplier.accept(flinkCommandArgs)) {
        acceptableSupplier.add(deploymentSupplier);
      }
    }

    if (acceptableSupplier.size() > 1) {
      throw new IllegalStateException("Multiple deployment suppliers are accepted.");
    }
    if (acceptableSupplier.isEmpty()) {
      throw new IllegalStateException("No deployment supplier found.");
    }

    DeploymentSupplier supplier = acceptableSupplier.get(0);
    supplier.configure(flinkCommandArgs, jobConfiguration);
    return supplier;
  }
}

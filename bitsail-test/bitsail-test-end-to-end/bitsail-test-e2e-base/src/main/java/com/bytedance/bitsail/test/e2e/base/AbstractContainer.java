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

package com.bytedance.bitsail.test.e2e.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;

import java.io.Closeable;
import java.io.IOException;

/**
 * Test containers for running BitSail job or simulating data source.
 */
public abstract class AbstractContainer implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractContainer.class);

  /**
   * Network of the container.
   */
  protected Network network;

  /**
   * Identifier for each container.
   */
  abstract String getContainerName();

  /**
   * Initialize network for the container.
   */
  abstract void initNetwork();

  /**
   * Terminate the container.
   */
  @Override
  public void close() throws IOException {
    LOG.info("Container {} is closed.", getContainerName());
  }
}

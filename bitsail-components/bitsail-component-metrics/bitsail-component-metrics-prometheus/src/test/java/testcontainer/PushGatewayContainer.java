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

package testcontainer;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;

public class PushGatewayContainer extends GenericContainer<PushGatewayContainer> {

  private static final int PUSH_GATEWAY_PORT = 9091;
  private static final int STARTUP_TIMEOUT = 5;

  public PushGatewayContainer(DockerImageName dockerImageName) throws UnknownHostException {
    super(dockerImageName);
    addExposedPort(PUSH_GATEWAY_PORT);
    String hostname = InetAddress.getLocalHost().getHostName();

    withCreateContainerCmdModifier(cmd -> cmd.withHostName(hostname));
    withLogConsumer(frame -> System.out.print(frame.getUtf8String()));
    withStartupTimeout(Duration.ofMinutes(STARTUP_TIMEOUT));
    withEnv("PUSH_GATEWAY_PORT", String.valueOf(PUSH_GATEWAY_PORT));

    setPortBindings(Collections.singletonList(String.format("%s:%s", PUSH_GATEWAY_PORT, PUSH_GATEWAY_PORT)));
  }
}

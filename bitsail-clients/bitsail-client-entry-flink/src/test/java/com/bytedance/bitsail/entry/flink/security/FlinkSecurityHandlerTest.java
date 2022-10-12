/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bytedance.bitsail.entry.flink.security;

import com.bytedance.bitsail.common.configuration.BitSailConfiguration;
import com.bytedance.bitsail.component.format.security.kerberos.option.KerberosOptions;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SecurityOptions;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class FlinkSecurityHandlerTest {

  @Test
  public void testDefaultConf() {
    String path = FlinkSecurityHandlerTest.class
        .getClassLoader().getResource("conf").getPath();
    Configuration flinkConfiguration = FlinkSecurityHandler.loadFlinkConfiguration(Paths.get(path));
    Assert.assertEquals("1", flinkConfiguration.getString("parallelism.default", null));
  }

  @Test
  public void testUserDefinedConf() {
    String path = FlinkSecurityHandlerTest.class.getClassLoader().getResource("test_dir/conf").getPath();
    Configuration flinkConfiguration = FlinkSecurityHandler.loadFlinkConfiguration(Paths.get(path));
    Assert.assertEquals("2", flinkConfiguration.getString("parallelism.default", null));
  }

  @Test
  public void testWriteFlinkConf() throws IOException {
    String path = FlinkSecurityHandlerTest.class
        .getClassLoader().getResource("conf").getPath();
    Configuration conf = FlinkSecurityHandler.loadFlinkConfiguration(Paths.get(path));
    Path tmpConfDir = FlinkSecurityHandler.writeConfToTmpFile(conf);

    File tmpConfFile = tmpConfDir.resolve("flink-conf.yaml").toFile();
    Assert.assertTrue(tmpConfFile.exists());
  }

  @Test
  public void testHandleGlobal() throws IOException {
    String workingDir = FlinkSecurityHandlerTest.class.getResource("/").getPath();

    BitSailConfiguration sysConfiguration = BitSailConfiguration.newDefault();
    sysConfiguration.set(KerberosOptions.KERBEROS_ENABLE, true);
    sysConfiguration.set(KerberosOptions.KERBEROS_KEYTAB_PATH, Paths.get(workingDir, "test.keytab").toString());
    sysConfiguration.set(KerberosOptions.KERBEROS_PRINCIPAL, "test_principal");
    sysConfiguration.set(KerberosOptions.KERBEROS_KRB5_CONF_PATH, (Paths.get(workingDir, "krb5.conf").toString()));

    ProcessBuilder processBuilder = new ProcessBuilder();
    FlinkSecurityHandler.processSecurity(sysConfiguration, processBuilder, Paths.get(workingDir));
    Map<String, String> environment = processBuilder.environment();
    Path flinkConfDir = Paths.get(environment.get("FLINK_CONF_DIR"));
    Assert.assertNotNull(flinkConfDir);
    Files.exists(flinkConfDir);
    Configuration flinkConfiguration = FlinkSecurityHandler.loadFlinkConfiguration(flinkConfDir);
    Assert.assertEquals(flinkConfiguration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB),
        sysConfiguration.get(KerberosOptions.KERBEROS_KEYTAB_PATH));
    Assert.assertEquals(flinkConfiguration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL),
        sysConfiguration.get(KerberosOptions.KERBEROS_PRINCIPAL));
  }
}

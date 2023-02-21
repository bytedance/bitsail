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

import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HistoryServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.bytedance.bitsail.entry.flink.handlers.CustomFlinkPackageHandler.writeConfToTmpFile;
import static com.bytedance.bitsail.entry.flink.utils.FlinkPackageResolver.loadFlinkConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CustomFlinkPackageHandlerTest {

  @Test
  public void testProcessSecurity() {
    String workingDir = getResourcePath("").toString();

    BitSailConfiguration sysConfiguration = BitSailConfiguration.newDefault();
    sysConfiguration.set(KerberosOptions.KERBEROS_ENABLE, true);
    sysConfiguration.set(KerberosOptions.KERBEROS_KEYTAB_PATH, Paths.get(workingDir, "test.keytab").toString());
    sysConfiguration.set(KerberosOptions.KERBEROS_PRINCIPAL, "test_principal");
    sysConfiguration.set(KerberosOptions.KERBEROS_KRB5_CONF_PATH, (Paths.get(workingDir, "krb5.conf").toString()));

    Configuration flinkConfiguration = new Configuration();

    CustomFlinkPackageHandler.processSecurity(sysConfiguration, flinkConfiguration);
    Assert.assertEquals(flinkConfiguration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB),
        sysConfiguration.get(KerberosOptions.KERBEROS_KEYTAB_PATH));
    Assert.assertEquals(flinkConfiguration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL),
        sysConfiguration.get(KerberosOptions.KERBEROS_PRINCIPAL));
  }

  @Test
  public void testProcessHistoryServer() {

    FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
    flinkCommandArgs.setHistoryServerEnable(true);
    flinkCommandArgs.setJobmanagerArchiveFsDir("test_dir");
    flinkCommandArgs.setHistoryServerWebAddress("test_address");
    flinkCommandArgs.setHistoryServerWebPort(100);
    flinkCommandArgs.setHistoryServerArchiveFsDir("test_dir2");
    flinkCommandArgs.setHistoryServerArchiveFsRefreshInterval(20000);
    Configuration flinkConfiguration = new Configuration();

    CustomFlinkPackageHandler.processHistoryServer(flinkCommandArgs, flinkConfiguration);
    Assert.assertEquals(flinkConfiguration.getString(JobManagerOptions.ARCHIVE_DIR),
        flinkCommandArgs.getJobmanagerArchiveFsDir());
    Assert.assertEquals(flinkConfiguration.getString(HistoryServerOptions.HISTORY_SERVER_WEB_ADDRESS),
        flinkCommandArgs.getHistoryServerWebAddress());
    Assert.assertEquals(flinkConfiguration.getInteger(HistoryServerOptions.HISTORY_SERVER_WEB_PORT),
        flinkCommandArgs.getHistoryServerWebPort());
    Assert.assertEquals(flinkConfiguration.getString(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_DIRS),
        flinkCommandArgs.getHistoryServerArchiveFsDir());
    Assert.assertEquals(flinkConfiguration.getLong(HistoryServerOptions.HISTORY_SERVER_ARCHIVE_REFRESH_INTERVAL),
        flinkCommandArgs.getHistoryServerArchiveFsRefreshInterval());
  }

  @Test
  public void testWriteFlinkConf() throws IOException {
    Path path = getResourcePath("conf");
    Configuration conf = loadFlinkConfiguration(path);
    Path tmpConfDir = writeConfToTmpFile(conf);

    File tmpConfFile = tmpConfDir.resolve("flink-conf.yaml").toFile();
    assertTrue(tmpConfFile.exists());
    Configuration tmpFlinkConfiguration = loadFlinkConfiguration(tmpConfDir);
    assertEquals("1", tmpFlinkConfiguration.getString("parallelism.default", null));
  }

  @SneakyThrows
  private Path getResourcePath(String resource) {
    return Paths.get(CustomFlinkPackageHandlerTest.class
        .getClassLoader()
        .getResource(resource)
        .toURI()
    );
  }
}

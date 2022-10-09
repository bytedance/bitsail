package com.bytedance.bitsail.client.api.command;

import org.junit.Assert;
import org.junit.Test;

public class KerberosCommandArgsParserTest {

  @Test
  public void testParseKerberosArgs() {
    String[] args = new String[]{
      "--keytab-path", "/etc/kerberos/keytab",
      "--principal", "test_user",
      "--krb5-conf-path", "/etc/kerberos/krb5.conf",
      "--unknownkey", "unknown_value"
    };

    BaseCommandArgs kerberosCommandArgs = new BaseCommandArgs();
    CommandArgsParser.parseArguments(args, kerberosCommandArgs);

    Assert.assertEquals("/etc/kerberos/keytab", kerberosCommandArgs.getKeytabPath());
    Assert.assertEquals("test_user", kerberosCommandArgs.getPrincipal());
    Assert.assertEquals("/etc/kerberos/krb5.conf", kerberosCommandArgs.getKrb5ConfPath());
    Assert.assertFalse(kerberosCommandArgs.isEnableKerberos());
  }

  @Test
  public void testParseMoreArgs() {
    String[] args = new String[]{
      "--enable-kerberos",
      "--keytab-path", "/root/dts_test/test.keytab",
      "--principal", "admin/admin@HADOOP.COM",
      "--krb5-conf-path", "/etc/krb5.conf"
    };
    BaseCommandArgs kerberosCommandArgs = new BaseCommandArgs();
    CommandArgsParser.parseArguments(args, kerberosCommandArgs);
    Assert.assertTrue(kerberosCommandArgs.isEnableKerberos());
  }
}

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

package com.bytedance.bitsail.connector.elasticsearch.base;

import org.junit.Assert;
import org.junit.Test;

public class NetUtilTest {

  @Test
  public void testIpv4() {
    String ipv4Address = "127.0.0.1:1234";
    Assert.assertTrue(NetUtil.isIpv4Address(ipv4Address));
    Assert.assertFalse(NetUtil.isIpv6Address(ipv4Address));

    Assert.assertEquals("127.0.0.1", NetUtil.getIpv4Ip(ipv4Address));
    Assert.assertEquals(1234, NetUtil.getIpv4Port(ipv4Address));
  }

  @Test
  public void testIpv6() {
    String ipv6Address = "[2001:db8:3333:4444:5555:6666:7777:8888]:1234";
    Assert.assertTrue(NetUtil.isIpv6Address(ipv6Address));
    Assert.assertFalse(NetUtil.isIpv4Address(ipv6Address));

    Assert.assertEquals("[2001:db8:3333:4444:5555:6666:7777:8888]", NetUtil.getIpv6Ip(ipv6Address));
    Assert.assertEquals(1234, NetUtil.getIpv6Port(ipv6Address));
  }

  @Test
  public void testAlias() {
    String ipv4Address = "localhost:1234";
    Assert.assertTrue(NetUtil.isIpv4Address(ipv4Address));
    Assert.assertFalse(NetUtil.isIpv6Address(ipv4Address));

    Assert.assertEquals("localhost", NetUtil.getIpv4Ip(ipv4Address));
    Assert.assertEquals(1234, NetUtil.getIpv4Port(ipv4Address));
  }
}

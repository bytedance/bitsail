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

import com.bytedance.bitsail.common.util.Preconditions;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NetUtil {

  private static final String IPV6_HTTP_PORT_FORMAT = "(\\[.*\\]):(\\d+)";

  public static boolean isIpv4Address(String h) {
    String[] tok = h.split(":");
    return tok.length == 2;
  }

  public static String getIpv4Ip(String h) {
    Preconditions.checkState(isIpv4Address(h));
    return h.split(":")[0];
  }

  public static int getIpv4Port(String h) {
    Preconditions.checkState(isIpv4Address(h));
    return Integer.parseInt(h.split(":")[1]);
  }

  public static boolean isIpv6Address(String h) {
    Matcher httpMatcher = Pattern.compile(IPV6_HTTP_PORT_FORMAT).matcher(h);
    return httpMatcher.find();
  }

  public static String getIpv6Ip(String h) {
    Preconditions.checkState(isIpv6Address(h));
    Matcher httpMatcher = Pattern.compile(IPV6_HTTP_PORT_FORMAT).matcher(h);
    return httpMatcher.find() ? httpMatcher.group(1) : null;
  }

  public static int getIpv6Port(String h) {
    Preconditions.checkState(isIpv6Address(h));
    Matcher httpMatcher = Pattern.compile(IPV6_HTTP_PORT_FORMAT).matcher(h);
    return httpMatcher.find() ? Integer.parseInt(httpMatcher.group(2)) : 0;
  }
}

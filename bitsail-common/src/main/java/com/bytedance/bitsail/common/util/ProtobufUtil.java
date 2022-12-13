/*
 * Copyright 2022-present ByteDance.
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

package com.bytedance.bitsail.common.util;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtobufUtil {
  public static final Logger LOG = LoggerFactory.getLogger(ProtobufUtil.class);

  public static Descriptors.Descriptor getDescriptor(byte[] descriptorBytes, String classname) throws Exception {
    Descriptors.Descriptor targetDescriptor = null;

    DescriptorProtos.FileDescriptorSet descriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(descriptorBytes);
    for (DescriptorProtos.FileDescriptorProto fileDescriptorProto : descriptorSet.getFileList()) {
      Descriptors.FileDescriptor fileDescriptor = Descriptors.FileDescriptor.buildFrom(fileDescriptorProto, new Descriptors.FileDescriptor[] {});
      for (Descriptors.Descriptor descriptor : fileDescriptor.getMessageTypes()) {
        if (classname.equals(descriptor.getName())) {
          targetDescriptor = descriptor;
          break;
        }
      }
    }
    return targetDescriptor;
  }
}



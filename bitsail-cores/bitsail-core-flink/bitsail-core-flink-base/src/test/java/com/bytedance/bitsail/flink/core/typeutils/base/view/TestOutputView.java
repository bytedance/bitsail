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

package com.bytedance.bitsail.flink.core.typeutils.base.view;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class TestOutputView extends DataOutputStream implements DataOutputView {

  public TestOutputView() {
    super(new ByteArrayOutputStream(4096));
  }

  public TestInputView getInputView() {
    ByteArrayOutputStream baos = (ByteArrayOutputStream) out;
    return new TestInputView(baos.toByteArray());
  }

  @Override
  public void skipBytesToWrite(int numBytes) throws IOException {
    for (int i = 0; i < numBytes; i++) {
      write(0);
    }
  }

  @Override
  public void write(DataInputView source, int numBytes) throws IOException {
    byte[] buffer = new byte[numBytes];
    source.readFully(buffer);
    write(buffer);
  }
}
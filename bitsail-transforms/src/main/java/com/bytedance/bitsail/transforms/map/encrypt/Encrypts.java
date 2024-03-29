/*
 * Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.bytedance.bitsail.transforms.map.encrypt;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;

public enum Encrypts {

  MD5 {
    @Override
    public String encrypt(String value) {
      return Hashing.md5().newHasher()
          .putString(value, Charsets.UTF_8)
          .hash()
          .toString();
    }
  };

  public String encrypt(String value) {
    throw new UnsupportedOperationException();
  }

}

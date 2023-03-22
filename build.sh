#!/usr/bin/env bash
#
# Copyright 2022-2023 Bytedance Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

set -e
profile=$1
if [[ -z $profile ]]; then
  profile="flink-1.11,flink-1.11-embedded"
fi
revision="0.2.0-SNAPSHOT" # modify ${revision} when version updated

echo "mvn profile = ${profile}"
mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true -U -P${profile}

# Copy bitsail files into `output` directory
rm -rf output
mkdir -p output

cp -r bitsail-dist/target/bitsail-dist-${revision}-bin/bitsail-archive-${revision}/* output/ || {
  echo 'cp bitsail-dist failed'
  exit 1
}

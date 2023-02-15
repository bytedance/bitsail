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

mvnProfile=flink-embedded
revision="0.2.0-SNAPSHOT"   # modify ${revision} when version updated

echo "mvn profile = ${mvnProfile}"
mvn clean package -pl bitsail-dist -am -Dmaven.test.skip=true -U -P${mvnProfile}

# Copy bitsail files into `output` directory
rm -rf output
mkdir -p output

cp -r bitsail-dist/target/bitsail-dist-${revision}-bin/bitsail-archive-${revision}/* output/ || { echo 'cp bitsail-dist failed' ; exit 1; }

# add hadoop dependency for embedded flink
if [[ ${mvnProfile} == "flink-embedded" ]]; then
  with_hadoop_version=$1
  if [[ ${with_hadoop_version} == "--with-hadoop2" ]]; then
    echo "Add flink-shaded-hadoop-2-uber dependency into embedded flink ..."
    HADOOP_UBER_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar
    wget -O output/embedded/flink/lib/flink-shaded-hadoop-2-uber.jar "${HADOOP_UBER_URL}"
  fi
  if [[ ${with_hadoop_version} == "--with-hadoop3" ]]; then
    echo "Add flink-shaded-hadoop-3-uber dependency into embedded flink ..."
    HADOOP_UBER_URL=https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/flink/flink-shaded-hadoop-3-uber/3.1.1.7.2.9.0-173-9.0/flink-shaded-hadoop-3-uber-3.1.1.7.2.9.0-173-9.0.jar
    COMMONS_CLI_URL=https://repo1.maven.org/maven2/commons-cli/commons-cli/1.5.0/commons-cli-1.5.0.jar
    wget -O output/embedded/flink/lib/flink-shaded-hadoop-3-uber.jar "${HADOOP_UBER_URL}"
    wget -O output/embedded/flink/lib/commons-cli.jar "${COMMONS_CLI_URL}"
  fi
fi


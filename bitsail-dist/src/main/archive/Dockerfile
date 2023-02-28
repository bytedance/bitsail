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

FROM flink:1.11.6-scala_2.11-java8
ENV USERLIB=/opt/bitsail
ENV HADOOP_UBER_URL=https://repository.cloudera.com/artifactory/cloudera-repos/org/apache/flink/flink-shaded-hadoop-2-uber/3.0.0-cdh6.3.0-7.0/flink-shaded-hadoop-2-uber-3.0.0-cdh6.3.0-7.0.jar
ENV COMMONS_CLI_URL=https://repo1.maven.org/maven2/commons-cli/commons-cli/1.5.0/commons-cli-1.5.0.jar
RUN mkdir -p $USERLIB
COPY libs/ $USERLIB/
RUN wget -nv -O /opt/flink/lib/flink-shaded-hadoop-2-uber.jar "${HADOOP_UBER_URL}";
RUN wget -nv -O /opt/flink/lib/commons-cli.jar "${COMMONS_CLI_URL}";
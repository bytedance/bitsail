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

# Flink 1.11.6 image with hadoop 2.7.5 dependencies.
# Online image: blockliu/flink-1.11.6-hadoop-2.7.5

FROM blockliu/flink:1.11-scala_2.11-java8

LABEL maintainer="96pengpeng@gmail.com"

ENV HADOOP_UBER_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar

RUN wget -nv -O /opt/flink/lib/flink-shaded-hadoop-2-uber.jar "${HADOOP_UBER_URL}";

RUN rm /opt/flink/conf/log4j-cli.properties
RUN cp /opt/flink/conf/log4j-console.properties /opt/flink/conf/log4j-cli.properties
RUN cp /opt/flink/conf/log4j-console.properties /opt/flink/conf/log4j.properties
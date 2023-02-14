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

# This setup will be added as an entry in ConfigMap and overwrite log4j.properties in /opt/flink/conf/ in TaskManager/JobManager pods.
# See more details in https://nightlies.apache.org/flink/flink-docs-release-1.11/ops/deployment/native_kubernetes.html#log-files."

echo -e "\nCheck if path of 'bitsail-dist/target/bitsail-dist-*-bin/bitsail-archive*/libs' and will exit if not exists."
get_full_path() {
  DIR_WILDCARD=$1
  for TMP_DIR in ${DIR_WILDCARD}
  do
    if [ -d "${TMP_DIR}" ]; then
      func_result="${TMP_DIR}"
      break
    fi
  done
}
get_full_path "bitsail-dist/target/bitsail-dist-*-bin/"
BITSAIL_DIST_BIN_DIR=$func_result
func_result=
get_full_path "${BITSAIL_DIST_BIN_DIR}bitsail-archive*/"
BITSAIL_DIR=${func_result}
EMBEDDED_FLINK_DIR=${BITSAIL_DIR}embedded/flink
LOG4J_PROPERTIES=${EMBEDDED_FLINK_DIR}/conf/log4j.properties
COPY_LOG4J_PROPERTIES=${EMBEDDED_FLINK_DIR}/conf/copy_log4j.properties

if [ -f $COPY_LOG4J_PROPERTIES ]; then
  mv $COPY_LOG4J_PROPERTIES $LOG4J_PROPERTIES
  echo "Replace '$LOG4J_PROPERTIES' with original copy '$COPY_LOG4J_PROPERTIES'"
fi

echo -e "\nAppend client flink's log4j.properties with ConsoleAppender setup. Saving original copy from '$LOG4J_PROPERTIES' to '$COPY_LOG4J_PROPERTIES'"
cp $LOG4J_PROPERTIES $COPY_LOG4J_PROPERTIES
echo "rootLogger.appenderRef.console.ref = ConsoleAppender
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n" >> $LOG4J_PROPERTIES
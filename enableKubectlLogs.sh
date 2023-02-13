#!/usr/bin/env bash

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

echo -e "\nAppend client flink's log4j.properties with ConsoleAppender setup."
cp ${EMBEDDED_FLINK_DIR}/conf/log4j.properties ${EMBEDDED_FLINK_DIR}/conf/copy_log4j.properties
echo "rootLogger.appenderRef.console.ref = ConsoleAppender
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n" >> ${EMBEDDED_FLINK_DIR}/conf/log4j.properties
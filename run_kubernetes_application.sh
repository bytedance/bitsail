#!/usr/bin/env bash

ABSOLUTE_PATH="$(pwd)"

echo -e "\nCheck if input arguments are valid."
usage() { echo "Usage: $0 [-c <JSON file>] [-i <Image name>]
  -c [Required] Confiuration json file: ...
  -i [Optional] Customized source image: ..."; exit 1; }
while getopts :c:i: flag
do
  case "${flag}" in
    c) CONF_PATH=${OPTARG};;
    i) SOURCE_IMAGE=${OPTARG};;
        *) usage;;
    esac
done
[ -z $CONF_PATH ] && usage \
  || echo "Configuration file path is set to '$CONF_PATH'"
[ ! -f $CONF_PATH ] && echo "'$CONF_PATH' does not exist." && exit 1
[ -z $SOURCE_IMAGE ] && SOURCE_IMAGE='flink:1.11.3-scala_2.11-java8' && echo "Image is empty. Set to default image '$SOURCE_IMAGE'" \
  || echo "Source image is  set to '$SOURCE_IMAGE'"
CONF_FILE=$(basename ${CONF_PATH}) >/dev/null

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
BITSAIL_LIBS_DIR=${BITSAIL_DIR}libs
[ -z ${BITSAIL_LIBS_DIR} ] && echo "ERROR. bitsail libs directory doesn't exist" && exit 1
EMBEDDED_FLINK_DIR=${BITSAIL_DIR}embedded/flink
[ -z ${EMBEDDED_FLINK_DIR} ] && echo "ERROR. embedded flink directory doesn't exist" && exit 1


echo -e "\nStart running minikube environment. It may run in several miniutes at the first time to pull down minikube image in Docker'"
# minikube start


echo -e "\nChecking if current user can do everything on kubernetes pods."
if [[ "$(kubectl auth can-i '*' pod)" == *"yes"* ]]; then
  echo " Succeed on kubestl pod access check: Current user can do anything on kubernetes pods."
else
  echo " Fail on pod access check: Not able to do anything on kubernetes pods. Please check if minikube is installed properly. Exit."
  exit 1
fi


SERVICE_ACCOUNT='kubernetes-bitsail-service-account'
CLUSTER_ROLE_BINDING='kubernetes-bitsail-cluster-role'
echo -e "\nCreate service account '${SERVICE_ACCOUNT}' and clusterrolebinding '${CLUSTER_ROLE_BINDING}' to avoid default account access issue."
if [[ "$(kubectl get serviceaccounts)" != *"${SERVICE_ACCOUNT}"* ]]; then
  kubectl create serviceaccount ${SERVICE_ACCOUNT}
fi
if [[ "$(kubectl get clusterrolebindings)" != *"${CLUSTER_ROLE_BINDING}"* ]]; then
  kubectl create clusterrolebinding ${CLUSTER_ROLE_BINDING} --clusterrole=edit --serviceaccount=default:${SERVICE_ACCOUNT} --namespace=default
fi


echo -e "\nRoute docker daemon to K8S pods"
eval $(minikube docker-env)


BITSAIL_IMAGE_REPOSITORY=bitsail-core
BITSAIL_IMAGE_TAG=appmode
echo -e "\nDelete old images of '${BITSAIL_IMAGE_REPOSITORY}:${BITSAIL_IMAGE_TAG}' from docker image list:"
docker images
OLD_BITSAIL_IMAGES=$(docker images | grep ${BITSAIL_IMAGE_REPOSITORY} | grep ${BITSAIL_IMAGE_TAG} | awk '{print $3}')
for old_bitsail_image in ${OLD_BITSAIL_IMAGES}
do
  docker rmi ${old_bitsail_image}
done


TEMP_BITSAIL_IMAGE_DIR=tmp_bitsail_image_dir
USR_CONF_DIR=usrconf
USR_LIBS_DIR=usrlibs
echo -e "\nCreate a temporary directory '${TEMP_BITSAIL_IMAGE_DIR}' to contain Dockerfile and other related files.
 Tree structure is as below:
 ${TEMP_BITSAIL_IMAGE_DIR}
 ├── Dockerfile
 ├── ${USR_CONF_DIR}
 |  └── ${CONF_FILE}
 └── ${USR_LIBS_DIR}
    ├── bitsail-core.jar
    ├── clients/*
    ├── connectors/*
    └── engines/*"
[ -n "$(ls -A ${TEMP_BITSAIL_IMAGE_DIR} 2>/dev/null)" ] && rm -r ${TEMP_BITSAIL_IMAGE_DIR}
mkdir ${TEMP_BITSAIL_IMAGE_DIR}
mkdir ${TEMP_BITSAIL_IMAGE_DIR}/${USR_LIBS_DIR}
mkdir ${TEMP_BITSAIL_IMAGE_DIR}/${USR_CONF_DIR}
cp $CONF_PATH ${TEMP_BITSAIL_IMAGE_DIR}/${USR_CONF_DIR}/
cp -r ${BITSAIL_LIBS_DIR}/* ${TEMP_BITSAIL_IMAGE_DIR}/${USR_LIBS_DIR}
echo "FROM ${SOURCE_IMAGE}
ENV IMAGE_USR_LIBS_PATH=\$FLINK_HOME/${USR_LIBS_DIR}
ENV IMAGE_USR_CONFIG_PATH=\$FLINK_HOME/${USR_CONF_DIR}
RUN mkdir -p \$IMAGE_USR_LIBS_PATH
RUN mkdir -p \$IMAGE_USR_CONFIG_PATH
COPY ${USR_LIBS_DIR}/ \$IMAGE_USR_LIBS_PATH/
COPY ${USR_CONF_DIR}/ \$IMAGE_USR_CONFIG_PATH/" > ${TEMP_BITSAIL_IMAGE_DIR}/Dockerfile
docker build -t ${BITSAIL_IMAGE_REPOSITORY}:${BITSAIL_IMAGE_TAG} ${TEMP_BITSAIL_IMAGE_DIR}


echo -e "\nNew docker ${BITSAIL_IMAGE_REPOSITORY}:${BITSAIL_IMAGE_TAG} is built in docker image list:"
docker images


echo -e "\nRevert docker daemon setup to local: "
eval $(minikube docker-env -u)


echo -e "\nAppend client flink's log4j.properties with ConsoleAppender setup. This setup will be added as an entry in ConfigMap and overwrite log4j.properties in /opt/flink/conf/ in TaskManager/JobManager pods. See more details in https://nightlies.apache.org/flink/flink-docs-release-1.11/ops/deployment/native_kubernetes.html#log-files."
cp ${EMBEDDED_FLINK_DIR}/conf/log4j.properties ${EMBEDDED_FLINK_DIR}/conf/copy_log4j.properties
echo "rootLogger.appenderRef.console.ref = ConsoleAppender
appender.console.name = ConsoleAppender
appender.console.type = CONSOLE
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yyyy-MM-dd HH:mm:ss,SSS} %-5p %-60c %x - %m%n" >> ${EMBEDDED_FLINK_DIR}/conf/log4j.properties


# TODO check and delete deployment of flink-app-job
# kubectl delete -n default deployment flink-app-job


echo -e "\nStart running bitsail command line on job '${CONF_PATH}':"
cd ${BITSAIL_DIR} || exit
./bin/bitsail run \
   --engine flink \
   -t kubernetes-application \
   --conf ${ABSOLUTE_PATH}/${CONF_PATH} \
   --execution-mode run-application \
   --deployment-mode kubernetes-application \
   --kubernetes.container.image ${BITSAIL_IMAGE_REPOSITORY}:${BITSAIL_IMAGE_TAG} \
   --kubernetes.jobmanager.cpu 0.25 \
   --kubernetes.taskmanager.cpu 0.5
cd ${ABSOLUTE_PATH} || exit


echo -e "\nRevert log4j.properties change"
mv ${EMBEDDED_FLINK_DIR}/conf/copy_log4j.properties ${EMBEDDED_FLINK_DIR}/conf/log4j.properties





# minikube stop
# minikube delete
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

# This script is for running built bitSail jar on local Kubernetes cluster.
# Prerequisites:
#  1. Local environment has built BitSail with build.sh
#  2. Local environment has minikube and kubectl installed
#         minikube installation: https://minikube.sigs.k8s.io/docs/start/
#         kubectl installation: https://kubernetes.io/docs/tasks/tools/#kubectl


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
echo "The content of configuration file is $( cat $CONF_PATH)"
CONF_BASE64=$( cat $CONF_PATH | base64 )
echo "The base64 encoded configuration is $CONF_BASE64"
[ -z $SOURCE_IMAGE ] && SOURCE_IMAGE='ysamchu/bitsail-kubernetes:0.1' && echo "No image argument is configured. Set to default image '$SOURCE_IMAGE'" \
  || echo "Source image is set to '$SOURCE_IMAGE'"
CONF_BASE64=$( cat $CONF_PATH | base64 )

echo -e "\nCheck if path of '../output' and will exit if not exists."
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
get_full_path "../output"
BITSAIL_DIR=$func_result
[ -z ${BITSAIL_DIR} ] && echo "ERROR. bitsail libs directory doesn't exist" && exit 1
echo "${BITSAIL_DIR} exists."
BITSAIL_LIBS_DIR=${BITSAIL_DIR}/libs


echo -e "\nStart running minikube environment. It may run in several minutes at the first time to pull down minikube image in Docker'."
minikube start 2> /dev/null || (echo "'minikube' is not found. You will need to install minikube in https://minikube.sigs.k8s.io/docs/start/" && exit 1)


echo -e "\nChecking if current user can do everything on kubernetes pods."
kubectl version --short 2> /dev/null || (echo "'kubectl' is not found. You will need to install kubectl in https://kubernetes.io/docs/tasks/tools/#kubectl" && exit 1)
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


echo -e "\nRoute docker daemon to K8S pods."
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


TEMP_BITSAIL_IMAGE_DIR=../tmp_bitsail_image_dir
USR_LIBS_DIR=usrlibs
echo -e "\nCreate a temporary directory '${TEMP_BITSAIL_IMAGE_DIR}' to contain Dockerfile and other related files.
 Tree structure is as below:
 ${TEMP_BITSAIL_IMAGE_DIR}
 ├── Dockerfile
 └── ${USR_LIBS_DIR}
    ├── bitsail-core.jar
    ├── clients/*
    ├── connectors/*
    └── engines/*"
[ -n "$(ls -A ${TEMP_BITSAIL_IMAGE_DIR} 2>/dev/null)" ] && rm -r ${TEMP_BITSAIL_IMAGE_DIR}
mkdir ${TEMP_BITSAIL_IMAGE_DIR}
mkdir ${TEMP_BITSAIL_IMAGE_DIR}/${USR_LIBS_DIR}
cp -r ${BITSAIL_LIBS_DIR}/* ${TEMP_BITSAIL_IMAGE_DIR}/${USR_LIBS_DIR}
echo "FROM ${SOURCE_IMAGE}
ENV IMAGE_USR_LIBS_PATH=\$FLINK_HOME/${USR_LIBS_DIR}
RUN mkdir -p \$IMAGE_USR_LIBS_PATH
COPY ${USR_LIBS_DIR}/ \$IMAGE_USR_LIBS_PATH/" > ${TEMP_BITSAIL_IMAGE_DIR}/Dockerfile
docker build -t ${BITSAIL_IMAGE_REPOSITORY}:${BITSAIL_IMAGE_TAG} ${TEMP_BITSAIL_IMAGE_DIR}


echo -e "\nNew docker ${BITSAIL_IMAGE_REPOSITORY}:${BITSAIL_IMAGE_TAG} is built in docker image list:"
docker images


echo -e "\nRevert docker daemon setup to local."
eval $(minikube docker-env -u)


echo -e "\nStart running bitsail command line on job '${CONF_PATH}':"
cd ${BITSAIL_DIR} || exit
./bin/bitsail run \
  --engine flink \
  --target kubernetes-application \
  --deployment-mode kubernetes-application \
  --execution-mode run-application \
  -p kubernetes.jobmanager.service-account=${SERVICE_ACCOUNT} \
  -p kubernetes.container.image=${BITSAIL_IMAGE_REPOSITORY}:${BITSAIL_IMAGE_TAG} \
  --conf-in-base64 ${CONF_BASE64}

echo "\n\nIf you see 'Create flink application cluster bitsail-job successfully' at the last line, feel free to
    1. Check POD status by commanding
         minikube dashboard
    2. Dump logs of JobManager/TaskManager by commanding
         kubectl logs -fl app=bitsail-job
    3. Access WebUI of JobManager by commanding
         kubectl port-forward <jobmanager name> 8081 (<jobmanager name> can be found as bitsail-job-xxxxxx in 'kubectl get pods')

Once the job is complete, you can
    1. Stop the Kubernetes environment by commanding
         minikube stop
    2. Delete the Kubernetes environment by commanding
         minikube delete"
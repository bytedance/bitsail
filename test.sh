#!/usr/bin/env bash

ABSOLUTE_PATH="$(pwd)"

echo -e "\nCheck if input arguments are valid."
usage() { echo "Usage: $0 [-i <Image name>]
  -c [Required] Confiuration json file: ...
  -i [Optional] Customized source image: ..."; exit 1; }
while getopts :i: flag
do
  case "${flag}" in
    i) SOURCE_IMAGE=${OPTARG};;
        *) usage;;
    esac
done
[ -z $SOURCE_IMAGE ] && SOURCE_IMAGE='flink:1.11.6-scala_2.11-java8' && echo "Image is empty. Set to default image '$SOURCE_IMAGE'" \
  || echo "Source image is  set to '$SOURCE_IMAGE'"

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


echo -e "\nRevert docker daemon setup to local: "
eval $(minikube docker-env -u)
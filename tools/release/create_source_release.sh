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

##
## Variables with defaults (if not overwritten by environment)
##
MVN=${MVN:-mvn}

# fail immediately
set -o errexit
set -o nounset
# print command before executing
set -o xtrace

CURR_DIR=`pwd`
if [[ `basename $CURR_DIR` != "bitsail" ]] ; then
  echo "You have to call the script from the bitsail/ dir"
  exit 1
fi

RELEASE_VERSION=`grep -A 1 "<revision>" pom.xml  | grep '<revision>' | sed -e 's/<revision>//' -e 's/<\/revision>//' -e 's/ //g'`

if [ -z "${RELEASE_VERSION}" ]; then
    echo "RELEASE_VERSION was not set."
    exit 1
fi

echo "RELEASE_VERSION=${RELEASE_VERSION}"

# Darwin is macos, otherwise linux
if [ "$(uname)" == "Darwin" ]; then
    SHASUM="shasum -a 512"
else
    SHASUM="sha512sum"
fi

ROOT_DIR=`pwd`
RELEASE_DIR=${ROOT_DIR}/src_release
CLONE_DIR=${RELEASE_DIR}/bitsail-tmp-clone

# create a temporary git clone to ensure that we have a pristine source release
git clone ${ROOT_DIR} ${CLONE_DIR}
cd ${CLONE_DIR}

rsync -a \
  --exclude ".git" --exclude ".gitignore" \
  --exclude ".github" --exclude "target" --exclude "output" --exclude "target"\
  --exclude ".idea" --exclude "website" --exclude "yarn.lock" --exclude "package.json"\
  . bitsail-$RELEASE_VERSION

# compress source code
tar czf ${RELEASE_DIR}/bitsail-${RELEASE_VERSION}.src.tgz bitsail-${RELEASE_VERSION}
# add gpg signature
gpg --armor --detach-sig ${RELEASE_DIR}/bitsail-${RELEASE_VERSION}.src.tgz
cd ${RELEASE_DIR}
$SHASUM bitsail-${RELEASE_VERSION}.src.tgz > bitsail-${RELEASE_VERSION}.src.tgz.sha512

cd ${CURR_DIR}
rm -rf ${CLONE_DIR}

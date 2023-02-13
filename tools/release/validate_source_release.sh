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

RELEASE_VERSION=`grep -A 1 "<revision>" pom.xml  | grep '<revision>' | sed -e 's/<revision>//' -e 's/<\/revision>//' -e 's/ //g'`

cd src_release
# verity the release source code
gpg --verify bitsail-${RELEASE_VERSION}.src.tgz.asc bitsail-${RELEASE_VERSION}.src.tgz
# build the source code from the tar ball
tar -zxvf bitsail-${RELEASE_VERSION}.src.tgz && cd bitsail-${RELEASE_VERSION} && mvn clean verify -pl bitsail-dist -am -U -DskipUT=true -DskipITCase=false
# remove the tmp directory
cd ../ && rm -rf bitsail-${RELEASE_VERSION}
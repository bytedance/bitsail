# Support multi-arch flink docker image

## Introduction

Recently, more and more users are using new MacBooks based on a M1 chip for develop. Unlike previous MacBook which were Intel based, M1 has a different architecture, _i.e._, `arm64` instead of Intel`s `amd64`.

However, some old version of flink (1.11, 1.12, 1.13) does not have images for `arm64`. So we have to build 'cross-cpu-arch' images at first.

To support E2E test on flink engine, we build two cross-cpu-arch images:

 - [`blockliu/flink:1.11.6-hadoop3`](https://hub.docker.com/layers/blockliu/flink/1.11.6-hadoop3/images/sha256-928e72248a5f21a02a35e26d6dcf690b93602c903873a64659819611553017ff?context=repo)
 - [`blockliu/flink:1.11.6-hadoop2`](https://hub.docker.com/layers/blockliu/flink/1.11.6-hadoop2/images/sha256-f5a0ddf0784210ca8ebbdccd457335313913fb67c9bc10c39bc51095c0ef6a1e?context=repo)

These two images support `linux/amd64` and `linux/arm64`.


## Build Guides

Here we show how to build a suitable flink image (only version 1.11, 1.12 and 1.13) for your OS/ARCH by using [**buildx**](https://github.com/docker/buildx).

### 1. Prepare Dockerfile

Firstly you should determine which version of flink to use.
For example, we use flink 1.11.6 with scala 2.11 and java 8:
```bash
FLINK_VERSION=1.11.6
JAVA_VERSION=8
SCALA_VERSION=2.11
```

Then you can get the specific docker file template and docker entrypoint from [flink-docker](https://github.com/apache/flink-docker).

Remember to replace necessary arguments in the docker file template, for example:

```dockerfile
###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

# Replace here if you want to other version of jdk like openjdk:11-jre
FROM openjdk:8-jre

# Install dependencies
RUN set -ex; \
  apt-get update; \
  apt-get -y install libsnappy1v5 gettext-base; \
  rm -rf /var/lib/apt/lists/*

# Grab gosu for easy step-down from root
ENV GOSU_VERSION 1.11
RUN set -ex; \
  wget -nv -O /usr/local/bin/gosu "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture)"; \
  wget -nv -O /usr/local/bin/gosu.asc "https://github.com/tianon/gosu/releases/download/$GOSU_VERSION/gosu-$(dpkg --print-architecture).asc"; \
  export GNUPGHOME="$(mktemp -d)"; \
  for server in ha.pool.sks-keyservers.net $(shuf -e \
                          hkp://p80.pool.sks-keyservers.net:80 \
                          keyserver.ubuntu.com \
                          hkp://keyserver.ubuntu.com:80 \
                          pgp.mit.edu) ; do \
      gpg --batch --keyserver "$server" --recv-keys B42F6819007F00F88E364FD4036A9C25BF357DD4 && break || : ; \
  done && \
  gpg --batch --verify /usr/local/bin/gosu.asc /usr/local/bin/gosu; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" /usr/local/bin/gosu.asc; \
  chmod +x /usr/local/bin/gosu; \
  gosu nobody true

# Configure Flink version
# Replace these links if you want to use other sub-version like flink-1.11.4
ENV FLINK_TGZ_URL=https://www.apache.org/dyn/closer.cgi?action=download&filename=flink/flink-1.11.6/flink-1.11.6-bin-scala_2.11.tgz \
    FLINK_ASC_URL=https://www.apache.org/dist/flink/flink-1.11.6/flink-1.11.6-bin-scala_2.11.tgz.asc \
    GPG_KEY=19F2195E1B4816D765A2C324C2EED7B111D464BA \
    CHECK_GPG=true

# Prepare environment
ENV FLINK_HOME=/opt/flink
ENV PATH=$FLINK_HOME/bin:$PATH
RUN groupadd --system --gid=9999 flink && \
    useradd --system --home-dir $FLINK_HOME --uid=9999 --gid=flink flink
WORKDIR $FLINK_HOME

# Install Flink
RUN set -ex; \
  wget -nv -O flink.tgz "$FLINK_TGZ_URL"; \
  \
  if [ "$CHECK_GPG" = "true" ]; then \
    wget -nv -O flink.tgz.asc "$FLINK_ASC_URL"; \
    export GNUPGHOME="$(mktemp -d)"; \
    for server in ha.pool.sks-keyservers.net $(shuf -e \
                            hkp://p80.pool.sks-keyservers.net:80 \
                            keyserver.ubuntu.com \
                            hkp://keyserver.ubuntu.com:80 \
                            pgp.mit.edu) ; do \
        gpg --batch --keyserver "$server" --recv-keys "$GPG_KEY" && break || : ; \
    done && \
    gpg --batch --verify flink.tgz.asc flink.tgz; \
    gpgconf --kill all; \
    rm -rf "$GNUPGHOME" flink.tgz.asc; \
  fi; \
  \
  tar -xf flink.tgz --strip-components=1; \
  rm flink.tgz; \
  \
  chown -R flink:flink .;

# Configure container
COPY docker-entrypoint.sh /
ENTRYPOINT ["/docker-entrypoint.sh"]
EXPOSE 6123 8081
CMD ["help"]
```

### 2. Build and Push Images

Place the docker file and entrypoint file in the same folder, then you can use the following commands to build images and push them to hub:
```bash
# for example, tag=1.11.6-scala_2.11-java8
docker buildx build --platform linux/amd64,linux/arm64 --push -t {your_docker_account}/flink:{tag} .
```

Then you can use the image on your arm64 based laptop.
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# ARG debian_buster_image_tag=8-jre-slim
# FROM openjdk:${debian_buster_image_tag}
FROM python:3.9-slim-buster

# -- Layer: OS + Python

ARG shared_workspace=/opt/workspace
# ARG python_version=3.7.12

# RUN mkdir -p ${shared_workspace} && \
#     apt-get update -y && \
#     apt-get install -y curl
# RUN cd /usr/bin && \
#     curl -O https://www.python.org/ftp/python/${python_version}/Python-${python_version}.tar.xz && \
#     tar -xf Python-${python_version}.tar.xz && cd Python-${python_version} && ./configure --with-ensurepip=install && make -j 8 &&\
#     ln -s /usr/bin/Python-${python_version}/python /usr/bin/python && \
#     ln -s /usr/bin/Python-${python_version}/python /usr/bin/python3 && \
#     rm -rf Python-${python_version}.tar.xz

# # Install OpenJDK-8
# RUN apt-get update && \
#     apt-get install -y software-properties-common && \
#     apt-get install -y openjdk-8-jdk && \
#     apt-get install -y ant && \
#     apt-get clean;
    
# # Fix certificate issues
# RUN apt-get update && \
#     apt-get install ca-certificates-java && \
#     apt-get clean && \
#     update-ca-certificates -f;

# # Setup JAVA_HOME -- useful for docker commandline
# ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
# RUN export JAVA_HOME

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME
ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]
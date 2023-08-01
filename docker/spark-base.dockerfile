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

FROM ubuntu:22.04

ARG shared_workspace=/opt/workspace
ARG spark_version=3.3.2
ARG hadoop_version=3
ARG HADOOP_S3_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.402
ARG SPARK_XML_VERSION=0.16.0

# Set up envs
ENV SHARED_WORKSPACE=${shared_workspace}
ENV SPARK_HOME /opt/spark
RUN mkdir ${SPARK_HOME}
ENV SPARK_MASTER_HOST localhost
ENV SPARK_MASTER_PORT 7077
ENV PYTHONPATH=$SPARK_HOME/python
ENV PYSPARK_PYTHON python3
ENV PYSPARK_DRIVER_PYTHON jupyter

VOLUME ${shared_workspace}

# Set up OS libraries
RUN apt-get update
RUN apt-get install -y openjdk-19-jdk-headless curl python3-pip maven
RUN pip3 install --upgrade pip && pip3 install pipenv

# Download Spark jar and set up PySpark
RUN curl https://archive.apache.org/dist/spark/spark-${spark_version}/spark-${spark_version}-bin-hadoop${hadoop_version}.tgz -o spark.tgz
RUN tar -xf spark.tgz && mv spark-${spark_version}-bin-hadoop${hadoop_version}/* ${SPARK_HOME}/
RUN rm spark.tgz && rm -rf spark-${spark_version}-bin-hadoop${hadoop_version}
RUN pip3 install pyspark==${spark_version}

# Add S3 jars
RUN curl https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_S3_VERSION}/hadoop-aws-${HADOOP_S3_VERSION}.jar -o ${SPARK_HOME}/jars/hadoop-aws-${HADOOP_S3_VERSION}.jar
RUN curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar -o ${SPARK_HOME}/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Add spark-xml jar
RUN curl https://repo1.maven.org/maven2/com/databricks/spark-xml_2.12/${SPARK_XML_VERSION}/spark-xml_2.12-${SPARK_XML_VERSION}.jar -o ${SPARK_HOME}/jars/spark-xml_2.12-${SPARK_XML_VERSION}.jar

# Set up master IP address and executor memory
RUN cp $SPARK_HOME/conf/spark-defaults.conf.template $SPARK_HOME/conf/spark-defaults.conf
RUN echo "spark.driver.memory 4g" >> $SPARK_HOME/conf/spark-defaults.conf
# This is to exhaust all memory on the local machine
RUN echo "spark.executor.memory 4g" >> $SPARK_HOME/conf/spark-defaults.conf

# The following libraries are needed when build this image with GeoPandas on Apple chip mac
RUN apt-get install -y gdal-bin libgdal-dev


# Required if run the cluster mode
RUN apt-get install -y openssh-client openssh-server
RUN systemctl enable ssh

# Enable nopassword ssh
RUN ssh-keygen -t rsa -f ~/.ssh/id_rsa -N ""
RUN cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
RUN chmod 600 ~/.ssh/authorized_keys

# Expose Spark ports: master UI, worker UI, and job UI
EXPOSE 8080
EXPOSE 8081
EXPOSE 4040

CMD bash

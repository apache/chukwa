#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cd /
tar zxvf glog-0.3.3.tar.gz
cd glog-0.3.3
./configure
make && make install

cd /
unzip master.zip
cd gflags-master
mkdir build && cd build
export CXXFLAGS="-fPIC" && cmake .. && make VERBOSE=1
make && make install

cd /
cd lmdb/libraries/liblmdb
make && make install

cd /opt
mv /apache-maven-3.3.9-bin.tar.gz .
tar xzf apache-maven-3.3.9-bin.tar.gz
ln -s apache-maven-3.3.9 maven

echo "export M2_HOME=/opt/maven" > /etc/profile.d/maven.sh
echo "PATH=/opt/maven/bin:${PATH}" >> /etc/profile.d/maven.sh
echo "export SPARK_HOME=/opt/apache/spark-2.1.0-bin-hadoop2.7" >> /etc/profile.d/maven.sh
echo "export HADOOP_HOME=/opt/apache/hadoop-2.7.2" >> /etc/profile.d/maven.sh

source /etc/profile.d/maven.sh

cp /tmp/Makefile.config /CaffeOnSpark/caffe-public/

#export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.121-0.b13.el7_3.x86_64

JAVA_HOME=$(ls /usr/lib/jvm | grep java-1.8.0-openjdk-1.8.0 | grep -v jre)
JAVA_HOME=/usr/lib/jvm/${JAVA_HOME}
cp $JAVA_HOME/include/linux/* $JAVA_HOME/include/

echo "INCLUDE_DIRS += $JAVA_HOME/include" >> /CaffeOnSpark/caffe-public/Makefile.config

cd /CaffeOnSpark/caffe-public/
make all

export SPARK_HOME=/opt/apache/spark-1.6.0-bin-hadoop2.6

cd ..
make build


#export SPARK_HOME=/opt/apache/spark-1.6.0-bin-hadoop2.6
#${SPARK_HOME}/sbin/start-master.sh
#export MASTER_URL=spark://1dafed1ac7bf:7077
#export SPARK_WORKER_INSTANCES=1
#export CORES_PER_WORKER=1
#export TOTAL_CORES=$((${CORES_PER_WORKER}*${SPARK_WORKER_INSTANCES}))
#${SPARK_HOME}/sbin/start-slave.sh -c $CORES_PER_WORKER -m 3G ${MASTER_URL}

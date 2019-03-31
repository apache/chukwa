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
FROM centos:7
MAINTAINER Apache
ENV container docker

RUN yum -y update && yum clean all

RUN (cd /lib/systemd/system/sysinit.target.wants/; for i in *; do [ $i == systemd-tmpfiles-setup.service ] || rm -f $i; done); \
rm -f /lib/systemd/system/multi-user.target.wants/*;\
rm -f /etc/systemd/system/*.wants/*;\
rm -f /lib/systemd/system/local-fs.target.wants/*; \
rm -f /lib/systemd/system/sockets.target.wants/*udev*; \
rm -f /lib/systemd/system/sockets.target.wants/*initctl*; \
rm -f /lib/systemd/system/basic.target.wants/*;\
rm -f /lib/systemd/system/anaconda.target.wants/*; \
rm -f /run/nologin


RUN yum install -y net-tools tar wget bind-utils ntpd java-1.8.0-openjdk which openssh-server openssh-clients lsof
RUN yum -y install epel-release java-1.8.0-openjdk-devel.x86_64
RUN yum groupinstall -y 'Development Tools'
RUN yum install -y protobuf-devel leveldb-devel snappy-devel opencv-devel boost-devel hdf5-devel
RUN yum install -y gflags-devel glog-devel lmdb-devel
RUN yum install -y gcc gcc-c++ numpy scipy cmake git python-devel
RUN yum install -y openblas openblas-devel atlas-devel

RUN mkdir -p /opt/apache

RUN mkdir -p /opt/apache/zookeeper && \
    curl -SL https://www.apache.org/dist/zookeeper/zookeeper-3.4.13/zookeeper-3.4.13.tar.gz | \
    tar -xzC /opt/apache/zookeeper --strip 1
RUN mkdir -p /opt/apache/hadoop && \
    curl -SL https://www.apache.org/dist/hadoop/common/hadoop-3.1.1/hadoop-3.1.1.tar.gz | \
    tar -xzC /opt/apache/hadoop --strip 1
RUN mkdir -p /opt/apache/hbase && \
    curl -SL https://www.apache.org/dist/hbase/1.4.9/hbase-1.4.9-bin.tar.gz | \
    tar -xzC /opt/apache/hbase --strip 1
RUN mkdir -p /opt/apache/solr && \
    curl -SL https://www.apache.org/dist/lucene/solr/5.5.5/solr-5.5.5.tgz | \
    tar -xzC /opt/apache/solr --strip 1
ADD target/chukwa-core-0.8.0.tar.gz /opt/apache
RUN rm -f chukwa-core*.tar.gz
RUN ln -s /opt/apache/chukwa-* /opt/apache/chukwa
RUN cp -f /opt/apache/chukwa/etc/chukwa/hadoop-log4j.properties /opt/apache/hadoop/etc/hadoop/log4j.properties
RUN cp -f /opt/apache/chukwa/etc/chukwa/hadoop-metrics2.properties /opt/apache/hadoop/etc/hadoop/hadoop-metrics2.properties
RUN cp -f /opt/apache/chukwa/etc/chukwa/hadoop-metrics2-hbase.properties /opt/apache/hbase/conf/hadoop-metrics2-hbase.properties
RUN cp -f /opt/apache/chukwa/etc/chukwa/hbase-log4j.properties /opt/apache/hbase/conf/log4j.properties
ADD hadoop/* /opt/apache/hadoop/etc/hadoop/
ADD hbase/* /opt/apache/hbase/conf/
ADD start-all.sh /etc/start-all.sh
ADD setup-image.sh /

EXPOSE 4080 50070 8088 16010 7574

CMD [ "/usr/lib/systemd/systemd" ]


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
RUN wget https://archive.apache.org/dist/spark/spark-1.6.0/spark-1.6.0-bin-hadoop2.6.tgz
RUN tar xf spark-1.6.0-bin-hadoop2.6.tgz -C /opt/apache
RUN ln -s /opt/apache/spark-* /opt/apache/spark

ADD Makefile.config /tmp/Makefile.config
ADD config-caffe.sh /tmp/config-caffe.sh
RUN mkdir -p /caffe-test/train
RUN mkdir -p /caffe-test/train/data
RUN mkdir -p /caffe-test/chukwa
RUN mkdir -p /caffe-test/tera

ADD tera.sh /caffe-test/tera/tera.sh

ADD makeImage.sh /caffe-test/train/makeImage.sh
ADD test_solver.prototxt /caffe-test/train/test_solver.prototxt
ADD train_test.prototxt /caffe-test/train/train_test.prototxt
ADD train.sh /caffe-test/train/train.sh
RUN wget https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/google-glog/glog-0.3.3.tar.gz
RUN wget https://github.com/schuhschuh/gflags/archive/master.zip
RUN git clone https://github.com/LMDB/lmdb
RUN wget http://www-eu.apache.org/dist/maven/maven-3/3.3.9/binaries/apache-maven-3.3.9-bin.tar.gz
RUN git clone https://github.com/yahoo/CaffeOnSpark.git --recursive
RUN bash /tmp/config-caffe.sh

RUN chmod 755 /caffe-test/train/train.sh
RUN chmod 755 /caffe-test/tera/tera.sh

RUN wget https://www.apache.org/dist/zookeeper/zookeeper-3.4.13/zookeeper-3.4.13.tar.gz
RUN wget https://www.apache.org/dist/hadoop/common/hadoop-3.1.0/hadoop-3.1.0.tar.gz 
RUN wget https://www.apache.org/dist/hbase/1.2.5/hbase-1.2.5-bin.tar.gz
RUN wget https://www.apache.org/dist/lucene/solr/5.5.4/solr-5.5.4.tgz
ADD chukwa-0.8.0.tar.gz /opt/apache/
RUN tar xf zookeeper-3.4.6.tar.gz -C /opt/apache
RUN tar xf hadoop-3.1.0.tar.gz -C /opt/apache
RUN tar xf hbase-1.2.5-bin.tar.gz -C /opt/apache
RUN tar xf solr-5.5.4.tgz -C /opt/apache
RUN rm -f zookeeper-*.tar.gz hadoop-*.tar.gz hbase-*.tar.gz solr-*.tgz
RUN ln -s /opt/apache/zookeeper-* /opt/apache/zookeeper
RUN ln -s /opt/apache/hadoop-* /opt/apache/hadoop
RUN ln -s /opt/apache/hbase-* /opt/apache/hbase
RUN ln -s /opt/apache/solr-* /opt/apache/solr
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


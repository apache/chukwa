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
FROM centos:6
MAINTAINER Apache
RUN yum install -y tar wget bind-utils ntpd java-1.7.0-openjdk which openssh-server openssh-clients
RUN mkdir -p /opt/apache
RUN wget https://www.apache.org/dist/zookeeper/zookeeper-3.4.6/zookeeper-3.4.6.tar.gz
RUN wget https://www.apache.org/dist/hadoop/common/hadoop-2.7.2/hadoop-2.7.2.tar.gz 
RUN wget https://www.apache.org/dist/hbase/1.2.1/hbase-1.2.1-bin.tar.gz
RUN wget https://www.apache.org/dist/lucene/solr/5.5.0/solr-5.5.0.tgz
ADD chukwa-0.8.0.tar.gz /opt/apache/
RUN tar xf zookeeper-3.4.6.tar.gz -C /opt/apache
RUN tar xf hadoop-2.7.2.tar.gz -C /opt/apache
RUN tar xf hbase-1.2.1-bin.tar.gz -C /opt/apache
RUN tar xf solr-5.5.0.tgz -C /opt/apache
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
RUN bash setup-image.sh
RUN rm -f /setup-image.sh
EXPOSE 4080 50070 8088 16010 7574
CMD ["/etc/start-all.sh"]

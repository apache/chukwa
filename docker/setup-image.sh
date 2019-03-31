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

rm -f /run/nologin

# Initialize Users
groupadd -g 123 hadoop
groupadd -g 201 hdfs
useradd -u 200 -g 123 zookeeper
useradd -u 201 -g 201 -G 123 hdfs
useradd -u 202 -g 123 yarn
useradd -u 203 -g 123 hbase
useradd -u 204 -g 123 solr
useradd -u 205 -g 123 chukwa

# Global SSH configuration
echo StrictHostKeyChecking no >> /etc/ssh/ssh_config

# Setup SSH configuration for service accounts
su - hdfs -c 'cat /dev/zero | ssh-keygen -t dsa -N ""'
su - yarn -c 'cat /dev/zero | ssh-keygen -t dsa -N ""'
su - hbase -c 'cat /dev/zero | ssh-keygen -t dsa -N ""'
su - chukwa -c 'cat /dev/zero | ssh-keygen -t dsa -N ""'
cp /home/hdfs/.ssh/id_dsa.pub /home/hdfs/.ssh/authorized_keys
cp /home/yarn/.ssh/id_dsa.pub /home/yarn/.ssh/authorized_keys
cp /home/hbase/.ssh/id_dsa.pub /home/hbase/.ssh/authorized_keys
cp /home/chukwa/.ssh/id_dsa.pub /home/chukwa/.ssh/authorized_keys
chown hdfs:hdfs /home/hdfs/.ssh/authorized_keys
chown yarn:hadoop /home/yarn/.ssh/authorized_keys
chown hbase:hadoop /home/hbase/.ssh/authorized_keys
chown chukwa:hadoop /home/chukwa/.ssh/authorized_keys

# Create symlinks for current version
ln -s /opt/apache/zookeeper-* /opt/apache/zookeeper
ln -s /opt/apache/hadoop-* /opt/apache/hadoop
ln -s /opt/apache/hbase-* /opt/apache/hbase
ln -s /opt/apache/solr-* /opt/apache/solr
ln -s /opt/apache/chukwa-* /opt/apache/chukwa
ln -s /opt/apache/chukwa/share/chukwa/lib/json-simple-*.jar /opt/apache/hadoop/share/hadoop/common/lib/json-simple.jar
ln -s /opt/apache/chukwa/share/chukwa/chukwa-*-client.jar /opt/apache/hadoop/share/hadoop/common/lib/chukwa-client.jar
ln -s /opt/apache/chukwa/share/chukwa/lib/json-simple-*.jar /opt/apache/hbase/lib/json-simple.jar
ln -s /opt/apache/chukwa/share/chukwa/chukwa-*-client.jar /opt/apache/hbase/lib/chukwa-client.jar
ln -s /opt/apache/zookeeper/conf /etc/zookeeper
ln -s /opt/apache/hadoop/etc/hadoop /etc/hadoop
ln -s /opt/apache/hbase/conf /etc/hbase
ln -s /opt/apache/chukwa/etc/chukwa /etc/chukwa

# ZooKeeper configuration
cat /opt/apache/zookeeper/conf/zoo_sample.cfg | \
sed -e 's:/tmp/zoo:/var/lib/zoo:' > /opt/apache/zookeeper/conf/zoo.cfg

# Configure Solr with Chukwa configuraiton
mv -f /opt/apache/chukwa/etc/solr/logs /opt/apache/solr/example/solr/logs

# Configure Chukwa configuration
cat /opt/apache/chukwa/etc/chukwa/chukwa-env.sh | \
sed -e 's:\${JAVA_HOME}:/usr/lib/jvm/jre:' | \
sed -e 's:\${HBASE_CONF_DIR}:/opt/apache/hbase/conf:' | \
sed -e 's:\${HADOOP_CONF_DIR}:/opt/apache/hadoop/etc/hadoop:' | \
sed -e 's:/tmp/chukwa/pidDir:/var/run/chukwa:' | \
sed -e 's:/tmp/chukwa/log:/var/log/chukwa:' > /tmp/chukwa-env.sh
cp -f /tmp/chukwa-env.sh /opt/apache/chukwa/etc/chukwa/chukwa-env.sh
rm -f /tmp/chukwa-env.sh
cp -f /opt/apache/chukwa/etc/chukwa/hadoop-log4j.properties /opt/apache/hadoop/etc/hadoop/log4j.properties
cp -f /opt/apache/chukwa/etc/chukwa/hadoop-metrics2.properties /opt/apache/hadoop/etc/hadoop/hadoop-metrics2.properties
cp -f /opt/apache/chukwa/etc/chukwa/hadoop-metrics2-hbase.properties /opt/apache/hbase/conf/hadoop-metrics2-hbase.properties
cp -f /opt/apache/chukwa/etc/chukwa/hbase-log4j.properties /opt/apache/hbase/conf/log4j.properties

# Initialize Service file permissions
mkdir -p /var/lib/zookeeper
mkdir -p /var/lib/hdfs
mkdir -p /var/lib/yarn
mkdir -p /var/lib/chukwa
mkdir -p /var/lib/solr
mkdir -p /var/run/hadoop
mkdir -p /var/run/hbase
mkdir -p /var/run/chukwa
mkdir -p /var/run/solr
mkdir -p /var/log/hadoop
mkdir -p /var/log/hadoop/hdfs
mkdir -p /var/log/hadoop/yarn
mkdir -p /var/log/hbase
mkdir -p /var/log/chukwa

chown -R zookeeper:hadoop /opt/apache/zookeeper* /var/lib/zookeeper
chmod 775 /var/run/hadoop
chown -R hdfs:hadoop /opt/apache/hadoop* /var/lib/hdfs /var/run/hadoop /var/log/hadoop
chown -R yarn:hadoop /var/log/hadoop/yarn
chown -R hbase:hadoop /opt/apache/hbase* /var/run/hbase /var/log/hbase
chown -R solr:hadoop /opt/apache/solr* /var/run/solr
chown -R chukwa:hadoop /opt/apache/chukwa* /var/lib/chukwa /var/run/chukwa /var/log/chukwa

# format HDFS
export JAVA_HOME=/usr/lib/jvm/jre
export HADOOP_CONF_DIR=/opt/apache/hadoop/etc/hadoop
export HBASE_CONF_DIR=/opt/apache/hbase/conf
export CHUKWA_CONF_DIR=/opt/apache/chukwa/etc/chukwa
systemctl status sshd 
su - hdfs -c '/opt/apache/hadoop/bin/hadoop namenode -format'
su - hdfs -c '/opt/apache/hadoop/sbin/start-all.sh'
su - zookeeper -c '/opt/apache/zookeeper/bin/zkServer.sh start'
SAFE_MODE=`su - hdfs -c '/opt/apache/hadoop/bin/hadoop dfsadmin -safemode get 2>/dev/null'`
while [ "$SAFE_MODE" == "Safe mode is ON" ]; do
  SAFE_MODE=`su - hdfs -c '/opt/apache/hadoop/bin/hadoop dfsadmin -safemode get 2>/dev/null'`
  sleep 3
done
su - hdfs -c '/opt/apache/hadoop/bin/hadoop fs -mkdir /hbase >/dev/null 2>&1'
su - hdfs -c '/opt/apache/hadoop/bin/hadoop fs -chown hbase:hadoop /hbase >/dev/null 2>&1'
su - hbase -c '/opt/apache/hbase/bin/start-hbase.sh'
sleep 5
su - hbase -c '/opt/apache/hbase/bin/hbase shell < /opt/apache/chukwa/etc/chukwa/hbase.schema'
sleep 5
su - hbase -c '/opt/apache/hbase/bin/stop-hbase.sh'
su - hdfs -c '/opt/apache/hadoop/sbin/stop-dfs.sh'
su - zookeeper -c '/opt/apache/zookeeper/bin/zkServer.sh stop'

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
export PATH=${PATH}:/opt/apache/hadoop/bin:/opt/apache/hbase/bin
export JAVA_HOME=/usr/lib/jvm/jre
export HADOOP_CONF_DIR=/opt/apache/hadoop/etc/hadoop
export HBASE_CONF_DIR=/opt/apache/hbase/conf
export CHUKWA_CONF_DIR=/opt/apache/chukwa/etc/chukwa
systemctl status sshd 
su - zookeeper -c '/opt/apache/zookeeper/bin/zkServer.sh start'
su - solr -c 'cd /opt/apache/solr; ./bin/solr start -cloud -z localhost:2181'
su - solr -c 'cd /opt/apache/solr; ./bin/solr create_collection -c chukwa -n chukwa'
su - hdfs -c '/opt/apache/hadoop/sbin/start-dfs.sh >/dev/null 2>&1'
su - yarn -c '/opt/apache/hadoop/sbin/start-yarn.sh >/dev/null 2>&1'
SAFE_MODE=`su - hdfs -c '/opt/apache/hadoop/bin/hadoop dfsadmin -safemode get 2>/dev/null'`
while [ "$SAFE_MODE" == "Safe mode is ON" ]; do
  SAFE_MODE=`su - hdfs -c '/opt/apache/hadoop/bin/hadoop dfsadmin -safemode get 2>/dev/null'`
  sleep 3
done
su - hbase -c '/opt/apache/hbase/bin/start-hbase.sh >/dev/null 2>&1'
su - chukwa -c '/opt/apache/chukwa/sbin/start-chukwa.sh'
echo
echo "Chukwa Docker container is ready."
echo "Use web browser to visit port 4080 for demo."
echo "Username: admin, password: admin"
echo
echo "Enjoy!"
bash

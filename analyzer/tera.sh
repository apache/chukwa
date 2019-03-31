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

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/apache/hadoop/bin:/opt/apache/hbase/bin
su hdfs -c "hadoop dfs -mkdir -p /user/hdfs"
while :
do
  su hdfs -c "hadoop jar /opt/apache/hadoop-3.1.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.0.jar teragen 100 /user/hdfs/terasort-input"
  su hdfs -c "hadoop jar /opt/apache/hadoop-3.1.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.1.0.jar terasort /user/hdfs/terasort-input /user/hdfs/terasort-output"
  su hdfs -c "hadoop dfs -rmr  -skipTrash /user/hdfs/terasort-input/"
  su hdfs -c "hadoop dfs -rmr  -skipTrash /user/hdfs/terasort-output/"
done

#!/usr/bin/env bash 
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

pid=$$

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/../libexec/chukwa-config.sh

pidFile=${CHUKWA_PID_DIR}/HourlyChukwaRecordRolling.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`ps ax | grep HourlyChukwaRecordRolling | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -gt 0 ]; then
      exit -1
  fi
fi

rm -f ${pidFile}

${JAVA_HOME}/bin/java -DAPP=hourlyRolling -Dlog4j.configuration=chukwa-log4j.properties -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_PID_DIR=${CHUKWA_PID_DIR} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -classpath ${CHUKWA_CONF_DIR}:${HADOOP_CONF_DIR}:${CLASSPATH}::${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${tools} org.apache.hadoop.chukwa.extraction.demux.HourlyChukwaRecordRolling rollInSequence true deleteRawdata true


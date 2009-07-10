#!/bin/sh
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

. "$bin"/chukwa-config.sh

function stop {
  echo -n "Shutting down HICC..."
  ${JPS} | grep HiccWebServer | grep -v grep | grep -o '[^ ].*'| cut -f 1 -d" " | xargs kill -TERM >&/dev/null
  echo "done"
  exit 0
}

trap stop SIGHUP SIGINT SIGTERM

if [ "X$1" = "Xstop" ]; then
  stop
fi

if [ ! -z "${CHUKWA_HICC_MAX_MEM}" ]; then
  MAX_MEM="-Xmx${CHUKWA_HICC_MAX_MEM}"
else
  MAX_MEM=""
fi
if [ ! -z "${CHUKWA_HICC_MIN_MEM}" ]; then
  MIN_MEM="-Xms${CHUKWA_HICC_MIN_MEM}"
else
  MIN_MEM=""
fi

WEB_SERVICE_COMMON=`ls ${CHUKWA_HOME}/lib/*.jar ${CHUKWA_HOME}/webapps/hicc.war`
WEB_SERVICE_COMMON=`echo ${WEB_SERVICE_COMMON}| sed 'y/ /:/'`

exec ${JAVA_HOME}/bin/java ${MAX_MEM} ${MIN_MEM} -DAPP=hicc \
                           -Dlog4j.configuration=chukwa-log4j.properties \
                           -DCHUKWA_HOME=${CHUKWA_HOME} \
                           -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} \
                           -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} \
                           -DCHUKWA_DATA_DIR=${CHUKWA_DATA_DIR} \
                           -classpath ${CHUKWA_CONF_DIR}:${HADOOP_CONF_DIR}:${CLASSPATH}:${CHUKWA_CORE}:${WEB_SERVICE_COMMON}:${HADOOP_JAR}:${HICC_JAR} org.apache.hadoop.chukwa.hicc.HiccWebServer

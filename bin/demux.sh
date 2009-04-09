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

if [ "$CHUKWA_IDENT_STRING" = "" ]; then
  export CHUKWA_IDENT_STRING="$USER"
fi


trap 'remove_cron;rm -f $CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-demux.sh.pid ${CHUKWA_PID_DIR}/DemuxManager.pid; exit 0' 1 2 15
echo "${pid}" > "$CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-demux.sh.pid"


function remove_cron {
    mkdir -p ${CHUKWA_HOME}/var/tmp >&/dev/null
    crontab -l | grep -v ${CHUKWA_HOME}/bin/hourlyRolling.sh > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    cat /tmp/cron.${CURRENT_DATE} | grep -v ${CHUKWA_HOME}/bin/dailyRolling.sh > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
    crontab ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
    rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
}

function add_cron {
    mkdir -p ${CHUKWA_HOME}/var/tmp >&/dev/null
    crontab -l > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    crontest=$?

    if [ "X${crontest}" != "X0" ]; then
      cat > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} << CRON
16 * * * * ${CHUKWA_HOME}/bin/hourlyRolling.sh --config ${CHUKWA_CONF_DIR} >& ${CHUKWA_LOG_DIR}/hourly.log
30 1 * * * ${CHUKWA_HOME}/bin/dailyRolling.sh --config ${CHUKWA_CONF_DIR} >& ${CHUKWA_LOG_DIR}/daily.log
CRON
    else
      grep -v "${CHUKWA_HOME}/bin/hourlyRolling.sh" ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}  | grep -v "${CHUKWA_HOME}/bin/dailyRolling.sh" > ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2
      mv ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}.2 ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
      cat >> ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} << CRON
16 * * * * ${CHUKWA_HOME}/bin/hourlyRolling.sh --config ${CHUKWA_CONF_DIR} >& ${CHUKWA_LOG_DIR}/hourly.log
30 1 * * * ${CHUKWA_HOME}/bin/dailyRolling.sh --config ${CHUKWA_CONF_DIR} >& ${CHUKWA_LOG_DIR}/daily.log
CRON
    fi

    # save crontab
    echo -n "Registering cron jobs.."
    crontab ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE} > /dev/null 2>&1
    rm -f ${CHUKWA_HOME}/var/tmp/cron.${CURRENT_DATE}
    echo "done"
}

if [ "X$1" = "Xstop" ]; then
  echo -n "Shutting down demux.sh..."
  kill -TERM `cat ${CHUKWA_PID_DIR}/DemuxManager.pid`
  echo "done"
  exit 0
fi


 add_cron
 
 # Run Demux
 ${JAVA_HOME}/bin/java -Djava.library.path=${JAVA_LIBRARY_PATH} -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DAPP=demux -Dlog4j.configuration=chukwa-log4j.properties -classpath ${CLASSPATH}:${CHUKWA_CORE}:${HADOOP_JAR}:${COMMON}:${tools}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.extraction.demux.DemuxManager 

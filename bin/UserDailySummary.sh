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

while getopts ":c:d:j:m:n:t:" OPTION
do
        case $OPTION in
        c)
                clusters=$OPTARG
                ;;
        d)
                day=$OPTARG
                ;;
        j)
                jobfile=$OPTARG
                ;;
        t)
                taskfile=$OPTARG
                ;;
        esac
done;

recType=UserDailySummary

if [ "$day" = "" ]; then
        day=`date -d yesterday +%Y%m%d`
fi

if [ "$clusters" = "" ]; then
        clusters=`${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -ls /chukwa/repos | grep "/chukwa/repos" | cut -f 4 -d "/"`
fi

for cluster in $clusters
do

        if [ "$jobfile" = "" ]; then
                jobfile=/chukwa/repos/${cluster}/JobData/${day}/*/*/*.evt
        fi

        if [ "$taskfile" = "" ]; then
                taskfile=/chukwa/repos/${cluster}/TaskData/${day}/*/*/*.evt
        fi

        pig=${CHUKWA_HOME}/script/pig/${recType}.pig
        uniqdir="/chukwa/pig/${cluster}_${recType}_${day}_`date +%s`"
        output="$uniqdir/${day}.D"

        "$JAVA_HOME"/bin/java -cp ${CHUKWA_CORE}:${HADOOP_JAR}:${HADOOP_CONF_DIR}:${CHUKWA_HOME}/contrib/chukwa-pig/lib/pig.jar org.apache.pig.Main -param chukwaCore=${CHUKWA_CORE} -param chukwaPig=${CHUKWA_HOME}/contrib/chukwa-pig/chukwa-pig.jar -param output=${output} -param jobfile=$jobfile -param taskfile=$taskfile -param TIMESTAMP=`date -d ${day} +%s`000 ${pig}

        cmd="${JAVA_HOME}/bin/java -DAPP=UserDailySummary -Dlog4j.configuration=chukwa-log4j.properties -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -classpath ${CHUKWA_HOME}/contrib/chukwa-pig/chukwa-pig.jar:${HADOOP_CONF_DIR}:${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.tools.PigMover ${cluster} ${recType} ${uniqdir}  ${output} /chukwa/postProcess/"
        echo $cmd
        $cmd

done

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

if [ "$1" = '-h' ]; then
        echo "command line options:"
        echo "  -c <space seperated cluster names> "
        echo "     optional. if no cluster specified, hadoop dfs command will be used to get all clusters"
        echo "  -d <yyyyMMdd>"
        echo "     optional. if no date is given, yesterday will be used"
        echo "  -h help"
        echo "  -m <space seperated metrics list>"
        echo "     optional. Default value is 'SystemMetrics Hadoop_dfs_namenode Hadoop_dfs_FSDirectory Hadoop_dfs_datanode Hadoop_rpc_metrics Hadoop_mapred_jobtracker Hadoop_jvm_metrics Hadoop_dfs_FSNamesystem Df'"
        echo "  -t times"
        echo "     optional. default value is '5 30 180 720'"
        echo "  -n <add|remove>"
        echo "     Add/remove cron jobs."
        echo "     optional."
        echo "     start: add cron entry"
        echo "     remove: remove cron entry"
        exit
fi

pid=$$

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/../libexec/chukwa-config.sh

while getopts ":c:d:m:n:t:" OPTION
do
        case $OPTION in
        c)
                clusters=$OPTARG
                ;;
        d)
                day=$OPTARG
                ;;
        m)
                metricsList=$OPTARG
                ;;
        n)
                croncmd=$OPTARG
                ;;
        t)
                timeList=$OPTARG
        esac
done;

if [ "$day" = "" ]; then
        day=`date -d yesterday +%Y%m%d`
fi

if [ "$clusters" = "" ]; then
        clusters=`${HADOOP_HOME}/bin/hadoop --config ${HADOOP_CONF_DIR} dfs -ls /chukwa/repos | grep "/chukwa/repos" | cut -f 4 -d "/"`
fi

if [ "$timeList" = "" ]; then
        timeList="5 30 180 720"
fi

if [ "$metricsList" = "" ]; then
        metricsList="SystemMetrics Hadoop_dfs_namenode Hadoop_dfs_FSDirectory Hadoop_dfs_datanode Hadoop_rpc_metrics Hadoop_mapred_jobtracker Hadoop_jvm_metrics Hadoop_dfs_FSNamesystem Df"
fi

# add or remove cron entry
if [ "$croncmd" != "" ]; then
        if [ "$croncmd" = "add" ]; then
                crontab -l | grep -v downSampling > /tmp/crons; echo "0 3 * * * ${CHUKWA_HOME}/bin/downSampling.sh --config ${CHUKWA_CONF_DIR}" >> /tmp/crons; crontab < /tmp/crons
        fi
        if [ "$croncmd" = "remove" ]; then
                crontab -l | grep -v downSampling > /tmp/crons; crontab < /tmp/crons
        fi
        crontab -l
        exit
fi

for cluster in $clusters
do
        for recType in $metricsList
        do
                for time in $timeList
                do
                        pig=${CHUKWA_HOME}/script/pig/${recType}.pig
                        timePeriod=`/usr/bin/expr $time \* 60000`;
                        newRecType=${recType}-${time}
                        input="/chukwa/repos/$cluster/$recType/$day/*/*/*.evt"
                        uniqdir="/chukwa/pig/${cluster}_${newRecType}_${day}_`date +%s`"
                        output="$uniqdir/${day}.D"

                        # echo chukwa_home:${CHUKWA_HOME} chukwa_conf:${CHUKWA_CONF_DIR} cluster:$cluster day:$day pig:$pig recType:$recType newRecType:$newRecType time:$time timePeriod:$timePeriod input:$input output:$output;
                        echo ${CHUKWA_CONF_DIR} cluster:$cluster day:$day pig:$pig recType:$recType newRecType:$newRecType time:$time timePeriod:$timePeriod input:$input output:$output;
                        cmd="${JAVA_HOME}/bin/java -DHADOOP_CONF_DIR=${HADOOP_CONF_DIR} -classpath ${CHUKWA_CORE}:${HADOOP_JAR}:${HADOOP_CONF_DIR}:${CHUKWA_HOME}/contrib/chukwa-pig/lib/pig.jar org.apache.pig.Main -param chukwaCore=${CHUKWA_CORE} -param chukwaPig=${CHUKWA_HOME}/contrib/chukwa-pig/chukwa-pig.jar -param input='${input}' -param output='${output}' -param recType='${newRecType}' -param timePeriod=${timePeriod} -param cluster=${cluster} ${pig}"
                        # echo $cmd
                        $cmd

                        cmd="${JAVA_HOME}/bin/java -DAPP=PigDownSampling -Dlog4j.configuration=chukwa-log4j.properties -DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -classpath ${CHUKWA_HOME}/contrib/chukwa-pig/chukwa-pig.jar:${HADOOP_CONF_DIR}:${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR} org.apache.hadoop.chukwa.tools.PigMover ${cluster} ${newRecType} ${uniqdir}  ${output} /chukwa/postProcess/"
                        # echo $cmd
                        $cmd
                done
        done
done

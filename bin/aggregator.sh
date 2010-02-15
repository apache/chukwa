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

pid=$$

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/chukwa-config.sh

if [ "$CHUKWA_IDENT_STRING" = "" ]; then
  export CHUKWA_IDENT_STRING="$USER"
fi

CHUKWA_OPTS="-DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml"
CLASS_OPTS="-classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR}"
JVM_OPTS="-DAPP=dbAdmin -Dlog4j.configuration=chukwa-log4j.properties ${CHUKWA_OPTS} ${CLASS_OPTS}"

if [ $# -lt 3 ]; then
  echo "$0: [command] [cluster] [date]"
  echo "[cluster] - cluster name in jdbc.conf"
  echo "[command] - create/aggregate"
  echo "            create    : create database partition for given date"
  echo "            aggregate : aggregate database partition for given date range" 
  exit 0
fi

CMD=$1
shift
CLUSTER=$1
shift

if [ $CMD = "create" ]; then
  EXP_DATE=$@
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 7
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 30
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 91
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 365
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 3650
fi

if [ $CMD = "aggregate" ]; then
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.Aggregator $@
fi
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

CHUKWA_OPTS="-DCHUKWA_HOME=${CHUKWA_HOME} -DCHUKWA_CONF_DIR=${CHUKWA_CONF_DIR} -DCHUKWA_LOG_DIR=${CHUKWA_LOG_DIR} -DDATACONFIG=${CHUKWA_CONF_DIR}/mdl.xml"
CLASS_OPTS="-classpath ${CLASSPATH}:${CHUKWA_CORE}:${COMMON}:${HADOOP_JAR}:${CHUKWA_CONF_DIR}"
JVM_OPTS="-DAPP=dbAdmin -Dlog4j.configuration=chukwa-log4j.properties ${CHUKWA_OPTS} ${CLASS_OPTS}"

if [ $# -lt 3 ]; then
  echo "$0: [cluster] [command] [date]"
  echo "[cluster] - cluster name in jdbc.conf"
  echo "[command] - create/aggregate"
  echo "            create    : create database partition for given date"
  echo "            aggregate : aggregate database partition for given date range" 
  exit 0
fi

CLUSTER=$1
shift
CMD=$1
shift

if [ $CMD = "create" ]; then
  EXP_DATE=$@
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 7
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 30
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 91
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 365
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.TableCreator ${EXP_DATE} 3650
fi

if [ $CMD = "aggregate" ]; then
  ${JAVA_HOME}/bin/java -DCLUSTER=${CLUSTER} ${JVM_OPTS} org.apache.hadoop.chukwa.database.Aggregator $@
fi

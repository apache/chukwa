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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/chukwa-config.sh

java=$JAVA_HOME/bin/java


min=`date +%M`

if [ "$CHUKWA_IDENT_STRING" = "" ]; then
  export CHUKWA_IDENT_STRING="$USER"
fi

# monitor agent
#pidFile=$CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-agent.sh.pid
#if [ -f $pidFile ]; then
#  pid=`head ${pidFile}`
#  ChildPIDRunningStatus=`ps ax | grep agent.sh | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
#  if [ $ChildPIDRunningStatus -lt 1 ]; then
#      HOSTNAME=`hostname`
#      echo "${HOSTNAME}: agent pid file exists, but process missing.  Restarting agent.sh."
#      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start agent.sh &
#  fi 
#fi


# monitor demux.sh
pidFile=${CHUKWA_PID_DIR}/DemuxManager.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`ps ax | grep DemuxManager | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: pid file exists, but process missing.  Restarting demux.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start demux.sh &
  fi
fi

# monitor postProcess.sh
pidFile=$CHUKWA_PID_DIR/PostProcessorManager.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`ps ax | grep PostProcessorManager | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: pid file exists, but process missing.  Restarting postProcess.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start postProcess.sh &
  fi
fi

# monitor archive.sh
pidFile=${CHUKWA_PID_DIR}/ArchiveManager.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`ps ax | grep ChukwaArchiveManager | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: pid file exists, but process missing.  Restarting postProcess.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start archive.sh &
  fi
fi



# monitor collector
pidFile=$CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-jettyCollector.sh.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`ps ax | grep jettyCollector.sh | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: collector pid file exists, but process missing.  Restarting jettyCollector.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start jettyCollector.sh &
  fi
fi

# monitor node activity data loader
pidFile=$CHUKWA_PID_DIR/PbsNodes-data-loader.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep Exec | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: PbsNodes-data-loader pid file exists, but process missing.  Restarting nodeActivityDataLoader.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start nodeActivityDataLoader.sh &
  fi
fi

# monitor system data loader
#pidFile=$CHUKWA_PID_DIR/Df-data-loader.pid
#if [ -f $pidFile ]; then
#  pid=`head ${pidFile}`
#  ChildPIDRunningStatus=`${JPS} | grep Exec | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
#  if [ $ChildPIDRunningStatus -lt 1 ]; then
#      HOSTNAME=`hostname`
#      echo "${HOSTNAME}: Df-data-loader pid file exists, but process missing.  Restarting systemDataLoader.sh."
#      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start systemDataLoader.sh &
#  fi
#fi

#pidFile=$CHUKWA_PID_DIR/Iostat-data-loader.pid
#if [ -f $pidFile ]; then
#  pid=`head ${pidFile}`
#  ChildPIDRunningStatus=`${JPS} | grep Exec | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
#  if [ $ChildPIDRunningStatus -lt 1 ]; then
#      HOSTNAME=`hostname`
#      echo "${HOSTNAME}: Iostat-data-loader pid file exists, but process missing.  Restarting systemDataLoader.sh."
#      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start systemDataLoader.sh &
#  fi
#fi

#pidFile=$CHUKWA_PID_DIR/Sar-data-loader.pid
#if [ -f $pidFile ]; then
#  pid=`head ${pidFile}`
#  ChildPIDRunningStatus=`${JPS} | grep Exec | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
#  if [ $ChildPIDRunningStatus -lt 1 ]; then
#      HOSTNAME=`hostname`
#      echo "${HOSTNAME}: Sar-data-loader pid file exists, but process missing.  Restarting systemDataLoader.sh."
#      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start systemDataLoader.sh &
#  fi
#fi

#pidFile=$CHUKWA_PID_DIR/Top-data-loader.pid
#if [ -f $pidFile ]; then
#  pid=`head ${pidFile}`
#  ChildPIDRunningStatus=`${JPS} | grep Exec | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
#  if [ $ChildPIDRunningStatus -lt 1 ]; then
#      HOSTNAME=`hostname`
#      echo "${HOSTNAME}: Top-data-loader pid file exists, but process missing.  Restarting systemDataLoader.sh."
#      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start systemDataLoader.sh &
#  fi
#fi

pidFile=$CHUKWA_PID_DIR/Ps-data-loader.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep Exec | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: Ps-data-loader pid file exists, but process missing.  Restarting systemDataLoader.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start systemDataLoader.sh &
  fi
fi

# monitor torque data loader
pidFile=$CHUKWA_PID_DIR/TorqueDataLoader.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`${JPS} | grep TorqueDataLoader | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: pid file exists, but process missing.  Restarting torqueDataLoader.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start torqueDataLoader.sh &
  fi
fi

# monitor dataSinkFiles.sh
pidFile=$CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-processSinkFiles.sh.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`ps ax | grep processSinkFiles.sh | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: pid file exists, but process missing.  Restarting processSinkFiles.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start processSinkFiles.sh &
  fi
fi

# monitor dbAdmin.sh
pidFile=$CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-dbAdmin.sh.pid
if [ -f $pidFile ]; then
  pid=`head ${pidFile}`
  ChildPIDRunningStatus=`ps ax | grep dbAdmin.sh | grep -v grep | grep -o "[^ ].*" | grep ${pid} | wc -l`
  if [ $ChildPIDRunningStatus -lt 1 ]; then
      HOSTNAME=`hostname`
      echo "${HOSTNAME}: pid file exists, but process missing.  Restarting dbAdmin.sh."
      "$bin/chukwa-daemon.sh" --config $CHUKWA_CONF_DIR start dbAdmin.sh &
  fi
fi

tenmin=`echo ${min} | cut -b 2-`
if [ "X${tenmin}" == "X0" ]; then
  if [ -d ${CHUKWA_LOG_DIR}/metrics ]; then
    ${CHUKWA_HOME}/tools/expire.sh 3 ${CHUKWA_LOG_DIR}/metrics nowait
  fi
fi





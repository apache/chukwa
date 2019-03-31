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

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
java=$JAVA_HOME/bin/java

. "$bin"/../libexec/chukwa-config.sh

# stop demux
pidFile=$CHUKWA_PID_DIR/DemuxManager.pid
if [ -f $pidFile ]; then
   echo -n "Shutting down demux.."
   DEMUX_PID=`head ${pidFile}`
   kill -1 ${DEMUX_PID} > /dev/null 2>&1
   for i in 1 2 5; do
       test_pid=`ps ax | grep ${DEMUX_PID} | grep -v grep | grep DemuxManager | wc -l`
       if [ $test_pid -ge 1 ]; then
           sleep $i
           kill -1 ${DEMUX_PID} > /dev/null 2>&1
       else
           break
       fi
   done
   test_pid=`ps ax | grep ${DEMUX_PID} | grep -v grep | grep DemuxManager | wc -l`
   if [ $test_pid -ge 1 ]; then
       kill -9 ${DEMUX_PID} > /dev/null 2>&1
   fi
   rm -f ${pidFile}
   rm -f $CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-demux.sh.pid
   echo "done"
else
  echo " no $pidFile"
fi


# stop postProcess
pidFile=$CHUKWA_PID_DIR/PostProcessorManager.pid
if [ -f $pidFile ]; then
   echo -n "Shutting down dp ..."
   POST_PROCESS_PID=`head ${pidFile}`
   kill -1 ${POST_PROCESS_PID} > /dev/null 2>&1
   for i in 1 2 5; do
       test_pid=`ps ax | grep ${POST_PROCESS_PID} | grep -v grep | grep PostProcessorManager | wc -l`
       if [ $test_pid -ge 1 ]; then
           sleep $i
           kill -1 ${POST_PROCESS_PID} > /dev/null 2>&1
       else
           break
       fi
   done
   test_pid=`ps ax | grep ${POST_PROCESS_PID} | grep -v grep | grep PostProcessorManager | wc -l`
   if [ $test_pid -ge 1 ]; then
       kill -9 ${POST_PROCESS_PID} > /dev/null 2>&1 
   fi
   rm -f ${pidFile}
   rm -f $CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-postProcess.sh.pid
   echo "done"
else
  echo " no $pidFile"
fi

# stop archive
pidFile=$CHUKWA_PID_DIR/ArchiveManager.pid
if [ -f $pidFile ]; then
   echo -n "Shutting down archive ..."
   POST_PROCESS_PID=`head ${pidFile}`
   kill -1 ${POST_PROCESS_PID} > /dev/null 2>&1
   for i in 1 2 5; do
       test_pid=`ps ax | grep ${POST_PROCESS_PID} | grep -v grep | grep ChukwaArchiveManager | wc -l`
       if [ $test_pid -ge 1 ]; then
           sleep $i
           kill -1 ${POST_PROCESS_PID} > /dev/null 2>&1
       else
           break
       fi
   done
   test_pid=`ps ax | grep ${POST_PROCESS_PID} | grep -v grep | grep ChukwaArchiveManager | wc -l`
   if [ $test_pid -ge 1 ]; then
       kill -9 ${POST_PROCESS_PID} > /dev/null 2>&1
   fi
   rm -f ${pidFile}
   rm -f $CHUKWA_PID_DIR/chukwa-$CHUKWA_IDENT_STRING-postProcess.sh.pid
   echo "done"
else
  echo " no $pidFile"
fi


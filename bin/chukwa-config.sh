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

# included in all the hadoop scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# resolve links - $0 may be a softlink

this="${BASH_SOURCE-$0}"

# convert relative path to absolute path
CHUKWA_LIBEXEC=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$CHUKWA_PREFIX/$script"

# the root of the Chukwa installation
export CHUKWA_HOME=`pwd -P ${CHUKWA_LIBEXEC}/..`

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
          then
              shift
              confdir=$1
              shift
              export CHUKWA_CONF_DIR=$confdir
    fi
fi

#check to see it is specified whether to use the slaves or the
# masters file
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        slavesfile=$1
        shift
        export CHUKWA_SLAVES="${CHUKWA_CONF_DIR}/$slavesfile"
    fi
fi

if [ -z ${CHUKWA_LOG_DIR} ]; then
    export CHUKWA_LOG_DIR="$CHUKWA_HOME/logs"
fi

if [ -z ${CHUKWA_PID_DIR} ]; then
    export CHUKWA_PID_DIR="${CHUKWA_HOME}/var/run"
fi

CHUKWA_VERSION=`cat ${CHUKWA_HOME}/share/chukwa/VERSION`

# Allow alternate conf dir location.
if [ -z "$CHUKWA_CONF_DIR" ]; then
    export CHUKWA_CONF_DIR="${CHUKWA_CONF_DIR:-$CHUKWA_HOME/etc/chukwa}"
fi

if [ -f "${CHUKWA_CONF_DIR}/chukwa-env.sh" ]; then
  . "${CHUKWA_CONF_DIR}/chukwa-env.sh"
fi

CHUKWA_CLASSPATH="${CHUKWA_HOME}/share/chukwa/*:${CHUKWA_HOME}/share/chukwa/lib/*"

export CURRENT_DATE=`date +%Y%m%d%H%M`

if [ -z "$JAVA_HOME" ] ; then
  echo ERROR! You forgot to set JAVA_HOME in CHUKWA_CONF_DIR/chukwa-env.sh   
fi


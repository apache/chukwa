# Set Chukwa-specific environment variables here.

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


# The only required environment variable is JAVA_HOME.  All others are
# optional.  When running a distributed configuration it is best to
# set JAVA_HOME in this file, so that it is correctly defined on
# remote nodes.

# The java implementation to use.  Required.
export JAVA_HOME=${JAVA_HOME}

# Optional
# The location of HBase Configuration directory.  For writing data to
# HBase, you need to set environment variable HBASE_CONF to HBase conf
# directory.
export HBASE_CONF_DIR="${HBASE_CONF_DIR}"

# Hadoop Configuration directory
export HADOOP_CONF_DIR="${HADOOP_CONF_DIR}"

# The location of chukwa data repository (in either HDFS or your local
# file system, whichever you are using)
export chukwaRecordsRepository="/chukwa/repos/"

# The directory where pid files are stored. CHUKWA_HOME/var/run by default.
export CHUKWA_PID_DIR=${TODO_CHUKWA_PID_DIR}

# The location of chukwa logs, defaults to CHUKWA_HOME/logs
export CHUKWA_LOG_DIR=${TODO_CHUKWA_LOG_DIR}

# The location to store chukwa data, defaults to CHUKWA_HOME/data
#export CHUKWA_DATA_DIR="${CHUKWA_HOME}/data"

# Instance name for chukwa deployment
export CHUKWA_IDENT_STRING=$USER

export JAVA_PLATFORM=Linux-i386-32
export JAVA_LIBRARY_PATH=${HADOOP_HOME}/lib/native/${JAVA_PLATFORM}

# Datatbase driver name for storing Chukwa Data.
export JDBC_DRIVER=${TODO_CHUKWA_JDBC_DRIVER}

# Database URL prefix for Database Loader.
export JDBC_URL_PREFIX=${TODO_CHUKWA_JDBC_URL_PREFIX}

# HICC Jetty Server heap memory settings 
# Specify min and max size of heap to JVM, e.g. 300M
export CHUKWA_HICC_MIN_MEM=
export CHUKWA_HICC_MAX_MEM=

# HICC Jetty Server port, defaults to 4080
#export CHUKWA_HICC_PORT=

export CLASSPATH=${CLASSPATH}:${HBASE_CONF_DIR}:${HADOOP_CONF_DIR}

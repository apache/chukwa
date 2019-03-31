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

export SPARK_HOME=/opt/apache/spark-1.6.0-bin-hadoop2.6
${SPARK_HOME}/sbin/start-master.sh
export MASTER_URL=spark://localhost:7077
export SPARK_WORKER_INSTANCES=1
export CORES_PER_WORKER=1
export TOTAL_CORES=$((${CORES_PER_WORKER}*${SPARK_WORKER_INSTANCES}))
${SPARK_HOME}/sbin/start-slave.sh -c $CORES_PER_WORKER -m 3G ${MASTER_URL}


export LD_LIBRARY_PATH=/CaffeOnSpark/caffe-public/distribute/lib:/CaffeOnSpark/caffe-distri/distribute/lib
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/cuda-7.0/lib64:/usr/local/mkl/lib/intel64/

sh ./makeImage.sh


/opt/apache/spark-1.6.0-bin-hadoop2.6/bin/spark-submit \
    --files /caffe-test/train/test_solver.prototxt,/caffe-test/train/train_test.prototxt \
    --conf spark.cores.max=1 \
    --conf spark.task.cpus=1 \
    --conf spark.driver.extraLibraryPath="${LD_LIBRARY_PATH}" \
    --conf spark.executorEnv.LD_LIBRARY_PATH="${LD_LIBRARY_PATH}" \
    --class com.yahoo.ml.caffe.CaffeOnSpark  \
    /CaffeOnSpark/caffe-grid/target/caffe-grid-0.1-SNAPSHOT-jar-with-dependencies.jar \
        -train \
        -features accuracy,loss -label label \
        -conf /caffe-test/train/test_solver.prototxt \
    -clusterSize 1 \
        -devices 1 \
    -connection ethernet \
        -model file:/caffe-test/train/test.model \
        -output file:/caffe-test/train/test_result

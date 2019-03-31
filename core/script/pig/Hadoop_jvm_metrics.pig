/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
register $chukwaCore
register $chukwaPig
define chukwaLoader org.apache.hadoop.chukwa.pig.ChukwaLoader();
define timePartition_Hadoop_jvm_metrics_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Hadoop_jvm_metrics_$timePeriod org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' ,'processName' , 'memHeapCommittedM', 'logFatal', 'threadsWaiting', 'gcCount', 'threadsBlocked', 'logError', 'logWarn', 'memNonHeapCommittedM', 'gcTimeMillis', 'memNonHeapUsedM', 'logInfo', 'memHeapUsedM', 'threadsNew', 'threadsTerminated', 'threadsTimedWaiting', 'threadsRunnable');
A_Hadoop_jvm_metrics_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Hadoop_jvm_metrics_$timePeriod = FOREACH A_Hadoop_jvm_metrics_$timePeriod GENERATE timePartition_Hadoop_jvm_metrics_$timePeriod(ts) as time ,fields#'csource' as g0 ,fields#'processName' as g1 , fields#'memHeapCommittedM' as f0, fields#'logFatal' as f1, fields#'threadsWaiting' as f2, fields#'gcCount' as f3, fields#'threadsBlocked' as f4, fields#'logError' as f5, fields#'logWarn' as f6, fields#'memNonHeapCommittedM' as f7, fields#'gcTimeMillis' as f8, fields#'memNonHeapUsedM' as f9, fields#'logInfo' as f10, fields#'memHeapUsedM' as f11, fields#'threadsNew' as f12, fields#'threadsTerminated' as f13, fields#'threadsTimedWaiting' as f14, fields#'threadsRunnable' as f15;
C_Hadoop_jvm_metrics_$timePeriod = group B_Hadoop_jvm_metrics_$timePeriod by (time,g0 ,g1 );
D_Hadoop_jvm_metrics_$timePeriod = FOREACH C_Hadoop_jvm_metrics_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , group.g1 , AVG(B_Hadoop_jvm_metrics_$timePeriod.f0) as f0, AVG(B_Hadoop_jvm_metrics_$timePeriod.f1) as f1, AVG(B_Hadoop_jvm_metrics_$timePeriod.f2) as f2, AVG(B_Hadoop_jvm_metrics_$timePeriod.f3) as f3, AVG(B_Hadoop_jvm_metrics_$timePeriod.f4) as f4, AVG(B_Hadoop_jvm_metrics_$timePeriod.f5) as f5, AVG(B_Hadoop_jvm_metrics_$timePeriod.f6) as f6, AVG(B_Hadoop_jvm_metrics_$timePeriod.f7) as f7, AVG(B_Hadoop_jvm_metrics_$timePeriod.f8) as f8, AVG(B_Hadoop_jvm_metrics_$timePeriod.f9) as f9, AVG(B_Hadoop_jvm_metrics_$timePeriod.f10) as f10, AVG(B_Hadoop_jvm_metrics_$timePeriod.f11) as f11, AVG(B_Hadoop_jvm_metrics_$timePeriod.f12) as f12, AVG(B_Hadoop_jvm_metrics_$timePeriod.f13) as f13, AVG(B_Hadoop_jvm_metrics_$timePeriod.f14) as f14, AVG(B_Hadoop_jvm_metrics_$timePeriod.f15) as f15;
-- describe D_Hadoop_jvm_metrics_$timePeriod;
-- dump D_Hadoop_jvm_metrics_$timePeriod;
store D_Hadoop_jvm_metrics_$timePeriod into '$output' using seqWriter_Hadoop_jvm_metrics_$timePeriod;

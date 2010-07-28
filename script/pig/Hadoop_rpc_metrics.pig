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
define timePartition_Hadoop_rpc_metrics_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Hadoop_rpc_metrics_$timePeriod org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' , 'getjobcounters_avg_time', 'gettaskdiagnostics_num_ops', 'getbuildversion_num_ops', 'getprotocolversion_num_ops', 'getsystemdir_num_ops', 'submitjob_avg_time', 'gettaskcompletionevents_avg_time', 'getjobprofile_num_ops', 'gettaskdiagnostics_avg_time', 'getjobstatus_avg_time', 'getbuildversion_avg_time', 'gettaskcompletionevents_num_ops', 'rpcprocessingtime_avg_time', 'submitjob_num_ops', 'getsystemdir_avg_time', 'getjobcounters_num_ops', 'getjobprofile_avg_time', 'getnewjobid_num_ops', 'getjobstatus_num_ops', 'heartbeat_num_ops', 'getprotocolversion_avg_time', 'heartbeat_avg_time', 'rpcprocessingtime_num_ops', 'getnewjobid_avg_time');
A_Hadoop_rpc_metrics_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Hadoop_rpc_metrics_$timePeriod = FOREACH A_Hadoop_rpc_metrics_$timePeriod GENERATE timePartition_Hadoop_rpc_metrics_$timePeriod(ts) as time ,fields#'csource' as g0 , fields#'getjobcounters_avg_time' as f0, fields#'gettaskdiagnostics_num_ops' as f1, fields#'getbuildversion_num_ops' as f2, fields#'getprotocolversion_num_ops' as f3, fields#'getsystemdir_num_ops' as f4, fields#'submitjob_avg_time' as f5, fields#'gettaskcompletionevents_avg_time' as f6, fields#'getjobprofile_num_ops' as f7, fields#'gettaskdiagnostics_avg_time' as f8, fields#'getjobstatus_avg_time' as f9, fields#'getbuildversion_avg_time' as f10, fields#'gettaskcompletionevents_num_ops' as f11, fields#'rpcprocessingtime_avg_time' as f12, fields#'submitjob_num_ops' as f13, fields#'getsystemdir_avg_time' as f14, fields#'getjobcounters_num_ops' as f15, fields#'getjobprofile_avg_time' as f16, fields#'getnewjobid_num_ops' as f17, fields#'getjobstatus_num_ops' as f18, fields#'heartbeat_num_ops' as f19, fields#'getprotocolversion_avg_time' as f20, fields#'heartbeat_avg_time' as f21, fields#'rpcprocessingtime_num_ops' as f22, fields#'getnewjobid_avg_time' as f23;
C_Hadoop_rpc_metrics_$timePeriod = group B_Hadoop_rpc_metrics_$timePeriod by (time,g0 );
D_Hadoop_rpc_metrics_$timePeriod = FOREACH C_Hadoop_rpc_metrics_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , AVG(B_Hadoop_rpc_metrics_$timePeriod.f0) as f0, AVG(B_Hadoop_rpc_metrics_$timePeriod.f1) as f1, AVG(B_Hadoop_rpc_metrics_$timePeriod.f2) as f2, AVG(B_Hadoop_rpc_metrics_$timePeriod.f3) as f3, AVG(B_Hadoop_rpc_metrics_$timePeriod.f4) as f4, AVG(B_Hadoop_rpc_metrics_$timePeriod.f5) as f5, AVG(B_Hadoop_rpc_metrics_$timePeriod.f6) as f6, AVG(B_Hadoop_rpc_metrics_$timePeriod.f7) as f7, AVG(B_Hadoop_rpc_metrics_$timePeriod.f8) as f8, AVG(B_Hadoop_rpc_metrics_$timePeriod.f9) as f9, AVG(B_Hadoop_rpc_metrics_$timePeriod.f10) as f10, AVG(B_Hadoop_rpc_metrics_$timePeriod.f11) as f11, AVG(B_Hadoop_rpc_metrics_$timePeriod.f12) as f12, AVG(B_Hadoop_rpc_metrics_$timePeriod.f13) as f13, AVG(B_Hadoop_rpc_metrics_$timePeriod.f14) as f14, AVG(B_Hadoop_rpc_metrics_$timePeriod.f15) as f15, AVG(B_Hadoop_rpc_metrics_$timePeriod.f16) as f16, AVG(B_Hadoop_rpc_metrics_$timePeriod.f17) as f17, AVG(B_Hadoop_rpc_metrics_$timePeriod.f18) as f18, AVG(B_Hadoop_rpc_metrics_$timePeriod.f19) as f19, AVG(B_Hadoop_rpc_metrics_$timePeriod.f20) as f20, AVG(B_Hadoop_rpc_metrics_$timePeriod.f21) as f21, AVG(B_Hadoop_rpc_metrics_$timePeriod.f22) as f22, AVG(B_Hadoop_rpc_metrics_$timePeriod.f23) as f23;
-- describe D_Hadoop_rpc_metrics_$timePeriod;
-- dump D_Hadoop_rpc_metrics_$timePeriod;
store D_Hadoop_rpc_metrics_$timePeriod into '$output' using seqWriter_Hadoop_rpc_metrics_$timePeriod;

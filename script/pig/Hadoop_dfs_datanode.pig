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
define timePartition_Hadoop_dfs_datanode_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Hadoop_dfs_datanode_$timePeriod org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' , 'replaceblockop_avg_time', 'block_verification_failures', 'readmetadataop_avg_time', 'blocks_read', 'blocks_written', 'readblockop_avg_time', 'writes_from_remote_client', 'blocks_replicated', 'replaceblockop_num_ops', 'bytes_read', 'bytes_written', 'writeblockop_num_ops', 'reads_from_remote_client', 'readblockop_num_ops', 'copyblockop_avg_time', 'blockreports_avg_time', 'heartbeats_num_ops', 'writeblockop_avg_time', 'reads_from_local_client', 'blocks_verified', 'writes_from_local_client', 'heartbeats_avg_time', 'copyblockop_num_ops', 'readmetadataop_num_ops', 'blocks_removed', 'blockreports_num_ops');
A_Hadoop_dfs_datanode_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Hadoop_dfs_datanode_$timePeriod = FOREACH A_Hadoop_dfs_datanode_$timePeriod GENERATE timePartition_Hadoop_dfs_datanode_$timePeriod(ts) as time ,fields#'hostname' as g0 , fields#'replaceblockop_avg_time' as f0, fields#'block_verification_failures' as f1, fields#'readmetadataop_avg_time' as f2, fields#'blocks_read' as f3, fields#'blocks_written' as f4, fields#'readblockop_avg_time' as f5, fields#'writes_from_remote_client' as f6, fields#'blocks_replicated' as f7, fields#'replaceblockop_num_ops' as f8, fields#'bytes_read' as f9, fields#'bytes_written' as f10, fields#'writeblockop_num_ops' as f11, fields#'reads_from_remote_client' as f12, fields#'readblockop_num_ops' as f13, fields#'copyblockop_avg_time' as f14, fields#'blockreports_avg_time' as f15, fields#'heartbeats_num_ops' as f16, fields#'writeblockop_avg_time' as f17, fields#'reads_from_local_client' as f18, fields#'blocks_verified' as f19, fields#'writes_from_local_client' as f20, fields#'heartbeats_avg_time' as f21, fields#'copyblockop_num_ops' as f22, fields#'readmetadataop_num_ops' as f23, fields#'blocks_removed' as f24, fields#'blockreports_num_ops' as f25;
C_Hadoop_dfs_datanode_$timePeriod = group B_Hadoop_dfs_datanode_$timePeriod by (time,g0 );
D_Hadoop_dfs_datanode_$timePeriod = FOREACH C_Hadoop_dfs_datanode_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , AVG(B_Hadoop_dfs_datanode_$timePeriod.f0) as f0, AVG(B_Hadoop_dfs_datanode_$timePeriod.f1) as f1, AVG(B_Hadoop_dfs_datanode_$timePeriod.f2) as f2, AVG(B_Hadoop_dfs_datanode_$timePeriod.f3) as f3, AVG(B_Hadoop_dfs_datanode_$timePeriod.f4) as f4, AVG(B_Hadoop_dfs_datanode_$timePeriod.f5) as f5, AVG(B_Hadoop_dfs_datanode_$timePeriod.f6) as f6, AVG(B_Hadoop_dfs_datanode_$timePeriod.f7) as f7, AVG(B_Hadoop_dfs_datanode_$timePeriod.f8) as f8, AVG(B_Hadoop_dfs_datanode_$timePeriod.f9) as f9, AVG(B_Hadoop_dfs_datanode_$timePeriod.f10) as f10, AVG(B_Hadoop_dfs_datanode_$timePeriod.f11) as f11, AVG(B_Hadoop_dfs_datanode_$timePeriod.f12) as f12, AVG(B_Hadoop_dfs_datanode_$timePeriod.f13) as f13, AVG(B_Hadoop_dfs_datanode_$timePeriod.f14) as f14, AVG(B_Hadoop_dfs_datanode_$timePeriod.f15) as f15, AVG(B_Hadoop_dfs_datanode_$timePeriod.f16) as f16, AVG(B_Hadoop_dfs_datanode_$timePeriod.f17) as f17, AVG(B_Hadoop_dfs_datanode_$timePeriod.f18) as f18, AVG(B_Hadoop_dfs_datanode_$timePeriod.f19) as f19, AVG(B_Hadoop_dfs_datanode_$timePeriod.f20) as f20, AVG(B_Hadoop_dfs_datanode_$timePeriod.f21) as f21, AVG(B_Hadoop_dfs_datanode_$timePeriod.f22) as f22, AVG(B_Hadoop_dfs_datanode_$timePeriod.f23) as f23, AVG(B_Hadoop_dfs_datanode_$timePeriod.f24) as f24, AVG(B_Hadoop_dfs_datanode_$timePeriod.f25) as f25;
-- describe D_Hadoop_dfs_datanode_$timePeriod;
-- dump D_Hadoop_dfs_datanode_$timePeriod;
store D_Hadoop_dfs_datanode_$timePeriod into '$output' using seqWriter_Hadoop_dfs_datanode_$timePeriod;

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
define timePartition_Hadoop_dfs_FSNamesystem_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Hadoop_dfs_FSNamesystem_$timePeriod org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' , 'BlocksTotalCapacityRemainingGBCapacityTotalGBCapacityUsedGBFilesTotalPendingReplicationBlocksScheduledReplicationBlocksTotalLoadUnderReplicatedBlocks');
A_Hadoop_dfs_FSNamesystem_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Hadoop_dfs_FSNamesystem_$timePeriod = FOREACH A_Hadoop_dfs_FSNamesystem_$timePeriod GENERATE timePartition_Hadoop_dfs_FSNamesystem_$timePeriod(ts) as time ,fields#'csource' as g0 , fields#'BlocksTotalCapacityRemainingGBCapacityTotalGBCapacityUsedGBFilesTotalPendingReplicationBlocksScheduledReplicationBlocksTotalLoadUnderReplicatedBlocks' as f0;
C_Hadoop_dfs_FSNamesystem_$timePeriod = group B_Hadoop_dfs_FSNamesystem_$timePeriod by (time,g0 );
D_Hadoop_dfs_FSNamesystem_$timePeriod = FOREACH C_Hadoop_dfs_FSNamesystem_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , AVG(B_Hadoop_dfs_FSNamesystem_$timePeriod.f0) as f0;
-- describe D_Hadoop_dfs_FSNamesystem_$timePeriod;
-- dump D_Hadoop_dfs_FSNamesystem_$timePeriod;
store D_Hadoop_dfs_FSNamesystem_$timePeriod into '$output' using seqWriter_Hadoop_dfs_FSNamesystem_$timePeriod;

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
%default START '1234567890';
SystemMetrics = load 'hbase://SystemMetrics' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('tags:cluster cpu:combined cpu:idle cpu:sys cpu:user disk:ReadBytes disk:Reads disk:WriteBytes disk:Writes system:LoadAverage.1 memory:FreePercent memory:UsedPercent network:RxBytes network:RxDropped network:RxErrors network:RxPackets network:TxBytes network:TxCollisions network:TxErrors network:TxPackets','-loadKey -gt $START -caster Utf8StorageConverter') AS (rowKey, cluster, cpuCombined, cpuIdle, cpuSys, cpuUser, diskReadBytes, diskReads, diskWriteBytes, diskWrites, LoadAverage, memoryFreePercent, memoryUsedPercent, networkRxBytes, networkRxDropped, networkRxErrors, networkRxPackets, networkTxBytes, networkTxCollisions, networkTxErrors, networkTxPackets);
CleanseBuffer = foreach SystemMetrics generate REGEX_EXTRACT($0,'^\\d+',0) as time, cluster, cpuCombined, cpuIdle, cpuSys, cpuUser, diskReadBytes, diskReads, diskWriteBytes, diskWrites, LoadAverage, memoryFreePercent, memoryUsedPercent, networkRxBytes, networkRxDropped, networkRxErrors, networkRxPackets, networkTxBytes, networkTxCollisions, networkTxErrors, networkTxPackets;
ConcatBuffer = foreach CleanseBuffer generate CONCAT(CONCAT($0, '-'), $1) as rowId, cpuCombined, cpuIdle, cpuSys, cpuUser, diskReadBytes, diskReads, diskWriteBytes, diskWrites, LoadAverage, memoryFreePercent, memoryUsedPercent, networkRxBytes, networkRxDropped, networkRxErrors, networkRxPackets, networkTxBytes, networkTxCollisions, networkTxErrors, networkTxPackets;
TimeSeries = GROUP ConcatBuffer BY rowId;
ComputeBuffer = FOREACH TimeSeries GENERATE group, AVG(ConcatBuffer.cpuCombined), AVG(ConcatBuffer.cpuIdle), AVG(ConcatBuffer.cpuSys), AVG(ConcatBuffer.cpuUser), AVG(ConcatBuffer.diskReadBytes), AVG(ConcatBuffer.diskReads), AVG(ConcatBuffer.diskWriteBytes), AVG(ConcatBuffer.diskWrites), AVG(ConcatBuffer.LoadAverage), AVG(ConcatBuffer.memoryFreePercent), AVG(ConcatBuffer.memoryUsedPercent), AVG(ConcatBuffer.networkRxBytes), AVG(ConcatBuffer.networkRxDropped), AVG(ConcatBuffer.networkRxErrors), AVG(ConcatBuffer.networkRxPackets), AVG(ConcatBuffer.networkTxBytes), AVG(ConcatBuffer.networkTxCollisions), AVG(ConcatBuffer.networkTxErrors), AVG(ConcatBuffer.networkTxPackets);
STORE ComputeBuffer INTO 'ClusterSummary' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('cpu:Combined cpu:Idle cpu:Sys cpu:User disk:ReadBytes disk:Reads disk:WriteBytes disk:Writes system:LoadAverage memory:FreePercent memory:UsedPercent network:RxBytes network:RxDropped network:RxErrors network:RxPackets network:TxBytes network:TxCollisions network:TxErrors network:TxPackets');
HDFSMetrics = load 'hbase://Hadoop' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('dfs_FSNamesystem:cluster dfs_FSNamesystem:CapacityTotalGB dfs_FSNamesystem:CapacityUsedGB dfs_FSNamesystem:CapacityRemainingGB dfs_FSNamesystem:BlockCapacity dfs_FSNamesystem:BlocksTotal dfs_FSNamesystem:MissingBlocks dfs_FSNamesystem:CorruptBlocks dfs_FSNamesystem:UnderReplicatedBlocks dfs_FSNamesystem:FilesTotal','-loadKey -gt $START -caster Utf8StorageConverter') AS (rowKey, cluster, CapacityTotalGB, CapacityUsedGB, CapacityRemainingGB, BlockCapacity, BlocksTotal, MissingBlocks, CorruptBlocks, UnderReplicatedBlocks, FilesTotal);
CleanseBuffer = foreach HDFSMetrics generate REGEX_EXTRACT($0,'^\\d+',0) as time, cluster, CapacityTotalGB, CapacityUsedGB, CapacityRemainingGB, BlockCapacity, BlocksTotal, MissingBlocks, CorruptBlocks, UnderReplicatedBlocks, FilesTotal;
ConcatBuffer = foreach CleanseBuffer generate CONCAT(CONCAT($0, '-'), $1) as rowId, CapacityTotalGB, CapacityUsedGB, CapacityRemainingGB, BlockCapacity, BlocksTotal, MissingBlocks, CorruptBlocks, UnderReplicatedBlocks, FilesTotal;
STORE ConcatBuffer INTO 'ClusterSummary' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('hdfs:CapacityTotalGB hdfs:CapacityUsedGB hdfs:CapacityRemainingGB hdfs:BlockCapacity hdfs:BlocksTotal hdfs:MissingBlocks hdfs:CorruptBlocks hdfs:UnderReplicatedBlocks hdfs:FilesTotal');
MapReduceMetrics = load 'hbase://Hadoop' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('mapred_tasktracker:cluster mapred_tasktracker:mapTaskSlots mapred_tasktracker:maps_running mapred_tasktracker:reduceTaskSlots mapred_tasktracker:reduces_running mapred_tasktracker:tasks_completed mapred_tasktracker:tasks_failed_ping mapred_tasktracker:tasks_failed_timeout mapred_jobtracker:occupied_map_slots','-loadKey -gt $START -caster Utf8StorageConverter') AS (rowKey, cluster, mapTaskSlots, mapsRunning, reduceTaskSlots, reduceRunning, tasksCompleted, tasksFailedPing, tasksFailedTimeout);
CleanseBuffer = foreach MapReduceMetrics generate REGEX_EXTRACT($0,'^\\d+',0) as time, cluster, mapTaskSlots, mapsRunning, reduceTaskSlots, reduceRunning, tasksCompleted, tasksFailedPing, tasksFailedTimeout;
GroupBuffer = foreach CleanseBuffer generate CONCAT(CONCAT($0, '-'), $1) as rowId, mapTaskSlots, mapsRunning, reduceTaskSlots, reduceRunning, tasksCompleted, tasksFailedPing, tasksFailedTimeout;
TimeSeries = GROUP GroupBuffer BY rowId;
MapReduceSummary = FOREACH TimeSeries GENERATE group, SUM(GroupBuffer.mapTaskSlots), SUM(GroupBuffer.mapsRunning), SUM(GroupBuffer.reduceTaskSlots), SUM(GroupBuffer.reduceRunning), SUM(GroupBuffer.tasksCompleted), SUM(GroupBuffer.tasksFailedPing), SUM(GroupBuffer.tasksFailedTimeout);
STORE MapReduceSummary INTO 'ClusterSummary' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('mapreduce:mapTaskSlots mapreduce:mapsRunning mapreduce:reduceTaskSlots mapreduce:reduceRunning mapreduce:tasksCompleted mapreduce:tasksFailedPing mapreduce:tasksFailedTimeout'); 

MapReduceMetrics = load 'hbase://Hadoop' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('mapred_jobtracker:cluster mapred_jobtracker:occupied_map_slots mapred_jobtracker:occupied_reduce_slots','-loadKey -gt $START -caster Utf8StorageConverter') AS (rowKey, cluster, occupiedMapSlots, occupiedReduceSlots);
CleanseBuffer = foreach MapReduceMetrics generate REGEX_EXTRACT($0,'^\\d+',0) as time, cluster, occupiedMapSlots, occupiedReduceSlots;
GroupBuffer = foreach CleanseBuffer generate CONCAT(CONCAT($0, '-'), $1) as rowId, occupiedMapSlots, occupiedReduceSlots;
TimeSeries = GROUP GroupBuffer BY rowId;
MapReduceSummary = FOREACH TimeSeries GENERATE group, SUM(GroupBuffer.occupiedMapSlots), SUM(GroupBuffer.occupiedReduceSlots);
STORE MapReduceSummary INTO 'ClusterSummary' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('mapreduce:occupiedMapSlots mapreduce:occupiedReduceSlots'); 

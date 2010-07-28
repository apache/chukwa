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
define timePartition_Hadoop_dfs_namenode_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Hadoop_dfs_namenode_$timePeriod org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' , 'createfileops', 'addblockops', 'safemodetime', 'syncs_avg_time', 'blockreport_avg_time', 'filesrenamed', 'getlistingops', 'deletefileops', 'transactions_num_ops', 'fsimageloadtime', 'blockscorrupted', 'getblocklocations', 'filescreated', 'blockreport_num_ops', 'syncs_num_ops', 'transactions_avg_time');
A_Hadoop_dfs_namenode_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Hadoop_dfs_namenode_$timePeriod = FOREACH A_Hadoop_dfs_namenode_$timePeriod GENERATE timePartition_Hadoop_dfs_namenode_$timePeriod(ts) as time ,fields#'csource' as g0 , fields#'createfileops' as f0, fields#'addblockops' as f1, fields#'safemodetime' as f2, fields#'syncs_avg_time' as f3, fields#'blockreport_avg_time' as f4, fields#'filesrenamed' as f5, fields#'getlistingops' as f6, fields#'deletefileops' as f7, fields#'transactions_num_ops' as f8, fields#'fsimageloadtime' as f9, fields#'blockscorrupted' as f10, fields#'getblocklocations' as f11, fields#'filescreated' as f12, fields#'blockreport_num_ops' as f13, fields#'syncs_num_ops' as f14, fields#'transactions_avg_time' as f15;
C_Hadoop_dfs_namenode_$timePeriod = group B_Hadoop_dfs_namenode_$timePeriod by (time,g0 );
D_Hadoop_dfs_namenode_$timePeriod = FOREACH C_Hadoop_dfs_namenode_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , AVG(B_Hadoop_dfs_namenode_$timePeriod.f0) as f0, AVG(B_Hadoop_dfs_namenode_$timePeriod.f1) as f1, AVG(B_Hadoop_dfs_namenode_$timePeriod.f2) as f2, AVG(B_Hadoop_dfs_namenode_$timePeriod.f3) as f3, AVG(B_Hadoop_dfs_namenode_$timePeriod.f4) as f4, AVG(B_Hadoop_dfs_namenode_$timePeriod.f5) as f5, AVG(B_Hadoop_dfs_namenode_$timePeriod.f6) as f6, AVG(B_Hadoop_dfs_namenode_$timePeriod.f7) as f7, AVG(B_Hadoop_dfs_namenode_$timePeriod.f8) as f8, AVG(B_Hadoop_dfs_namenode_$timePeriod.f9) as f9, AVG(B_Hadoop_dfs_namenode_$timePeriod.f10) as f10, AVG(B_Hadoop_dfs_namenode_$timePeriod.f11) as f11, AVG(B_Hadoop_dfs_namenode_$timePeriod.f12) as f12, AVG(B_Hadoop_dfs_namenode_$timePeriod.f13) as f13, AVG(B_Hadoop_dfs_namenode_$timePeriod.f14) as f14, AVG(B_Hadoop_dfs_namenode_$timePeriod.f15) as f15;
-- describe D_Hadoop_dfs_namenode_$timePeriod;
-- dump D_Hadoop_dfs_namenode_$timePeriod;
store D_Hadoop_dfs_namenode_$timePeriod into '$output' using seqWriter_Hadoop_dfs_namenode_$timePeriod;

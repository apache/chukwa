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
define timePartition_Hadoop_mapred_jobtracker_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Hadoop_mapred_jobtracker_$timePeriod org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' , 'reduces_completed', 'maps_launched', 'jobs_completed', 'reduces_launched', 'maps_completed', 'jobs_submitted');
A_Hadoop_mapred_jobtracker_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Hadoop_mapred_jobtracker_$timePeriod = FOREACH A_Hadoop_mapred_jobtracker_$timePeriod GENERATE timePartition_Hadoop_mapred_jobtracker_$timePeriod(ts) as time ,fields#'csource' as g0 , fields#'reduces_completed' as f0, fields#'maps_launched' as f1, fields#'jobs_completed' as f2, fields#'reduces_launched' as f3, fields#'maps_completed' as f4, fields#'jobs_submitted' as f5;
C_Hadoop_mapred_jobtracker_$timePeriod = group B_Hadoop_mapred_jobtracker_$timePeriod by (time,g0 );
D_Hadoop_mapred_jobtracker_$timePeriod = FOREACH C_Hadoop_mapred_jobtracker_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , AVG(B_Hadoop_mapred_jobtracker_$timePeriod.f0) as f0, AVG(B_Hadoop_mapred_jobtracker_$timePeriod.f1) as f1, AVG(B_Hadoop_mapred_jobtracker_$timePeriod.f2) as f2, AVG(B_Hadoop_mapred_jobtracker_$timePeriod.f3) as f3, AVG(B_Hadoop_mapred_jobtracker_$timePeriod.f4) as f4, AVG(B_Hadoop_mapred_jobtracker_$timePeriod.f5) as f5;
-- describe D_Hadoop_mapred_jobtracker_$timePeriod;
-- dump D_Hadoop_mapred_jobtracker_$timePeriod;
store D_Hadoop_mapred_jobtracker_$timePeriod into '$output' using seqWriter_Hadoop_mapred_jobtracker_$timePeriod;

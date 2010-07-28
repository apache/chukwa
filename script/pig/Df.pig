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
define timePartition_Df_$timePeriod org.apache.hadoop.chukwa.TimePartition('$timePeriod');
define seqWriter_Df_$timePeriod org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','c_recordtype', 'c_application', 'c_cluster','c_source' ,'Mounted on' , 'Available', 'Use%', 'Used');
A_Df_$timePeriod = load '$input' using  chukwaLoader as (ts: long,fields);
B_Df_$timePeriod = FOREACH A_Df_$timePeriod GENERATE timePartition_Df_$timePeriod(ts) as time ,fields#'csource' as g0 ,fields#'Mounted on' as g1 , fields#'Available' as f0, fields#'Use%' as f1, fields#'Used' as f2;
C_Df_$timePeriod = group B_Df_$timePeriod by (time,g0 ,g1 );
D_Df_$timePeriod = FOREACH C_Df_$timePeriod generate group.time as ts, '$recType', 'downsampling $timePeriod', '$cluster', group.g0 , group.g1 , AVG(B_Df_$timePeriod.f0) as f0, AVG(B_Df_$timePeriod.f1) as f1, AVG(B_Df_$timePeriod.f2) as f2;
-- describe D_Df_$timePeriod;
-- dump D_Df_$timePeriod;
store D_Df_$timePeriod into '$output' using seqWriter_Df_$timePeriod;

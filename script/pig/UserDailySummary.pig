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

define seqWriter org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp','userid', 'totalJobs', 'dataLocalMaps', 'rackLocalMaps', 'remoteMaps', 'mapInputBytes', 'reduceOutputRecords', 'mapSlotHours', 'reduceSlotHours', 'totalMaps', 'totalReduces','c_recordtype','c_source','c_application', 'c_cluster');

/*****************************************************************/
/*********************** Task data begin *************************/
taskInputData = load '$taskfile' using  org.apache.hadoop.chukwa.pig.ChukwaLoader() as (ts: long,fields);

-- convert map to row-column
taskTable = FOREACH taskInputData GENERATE fields#'JOBID' as jobId, fields#'TASKID' as taskId, fields#'START_TIME' as startTime, fields#'FINISH_TIME' as finishTime, fields#'TASK_TYPE' as taskType;

-- group by taskId. get startTime and finishTime
taskGroup = group taskTable  by (jobId, taskId, taskType);
TaskTime = foreach taskGroup generate group, (MAX(taskTable.finishTime) - MAX(taskTable.startTime)) as slotHours;

-- taskTypeGroup
--taskTypeGroup =

-- group by jobId
taskJobGroup = group TaskTime by ($0.jobId, $0.taskType);
taskJobTime = foreach taskJobGroup generate group.jobId as jobId, group.taskType as taskType, SUM(TaskTime.slotHours) as slotHours;

-- seperate map and reduce slotHours
mapTaskTime = filter taskJobTime by taskType eq 'MAP';
reduceTaskTime = filter taskJobTime by taskType eq 'REDUCE';
/*********************** Task data end *************************/
/*****************************************************************/


/*****************************************************************/
/*********************** Job data begin *************************/
jobInputData = load '$jobfile' using  org.apache.hadoop.chukwa.pig.ChukwaLoader() as (ts: long,fields);

-- convert map to row-column
jobTable = FOREACH jobInputData GENERATE fields#'USER' as user, fields#'JOBID' as jobId, fields#'SUBMIT_TIME' as submitTime, fields#'FINISH_TIME' as finishTime, fields#'JOB_STATUS' as jobStatus, fields#'Counter:org.apache.hadoop.mapreduce.JobCounter:DATA_LOCAL_MAPS' as dataLocalMaps, fields#'Counter:org.apache.hadoop.mapreduce.JobCounter:RACK_LOCAL_MAPS' as rackLocalMaps, fields#'Counter:org.apache.hadoop.mapreduce.JobCounter:REMOTE_MAPS' as remoteMaps, fields#'TOTAL_MAPS' as totalMaps, fields#'TOTAL_REDUCES' as totalReduces, fields#'Counter:org.apache.hadoop.mapred.Task\$Counter:MAP_INPUT_BYTES' as mapInputBytes, fields#'Counter:org.apache.hadoop.mapred.Task\$Counter:REDUCE_OUTPUT_RECORDS' as reduceOutputRecords;

-- load data from a text file.
--jobTable = load '$jobfile' using PigStorage(',') as (user, jobId, submitTime, finishTime, jobStatus, dataLocalMaps, rackLocalMaps, remoteMaps, totalMaps, totalReduces, mapInputBytes, reduceOutputRecords);

-- find job and user
UserRecords = filter jobTable by user is not null;
JobUserGroup = group UserRecords by (jobId, user);
JobUser = foreach JobUserGroup generate group.jobId as jobId, group.user as user;

-- group by jobId. get submitTime and finishTime
jobGroup = group jobTable by jobId;
JobTime = foreach jobGroup generate group as jobId, MAX(jobTable.submitTime) as submitTime, MAX(jobTable.finishTime) as finishTime, MAX(jobTable.dataLocalMaps) as dataLocalMaps, MAX(jobTable.rackLocalMaps) as rackLocalMaps, MAX(jobTable.remoteMaps) as remoteMaps, MAX(jobTable.totalMaps) as totalMaps, MAX(jobTable.totalReduces) as totalReduces, MAX(jobTable.mapInputBytes) as mapInputBytes, MAX(jobTable.reduceOutputRecords) as reduceOutputRecords;

-- job status
finishedJobs = filter jobTable by jobStatus eq 'SUCCESS' or jobStatus eq 'KILLED' or jobStatus eq 'FAILED';
jobStatusRecords = foreach finishedJobs generate jobId, jobStatus;
distinctJobStatus = distinct jobStatusRecords;
/*********************** Job data end *************************/
/*****************************************************************/


-- Join job and task
JoinedJobTask = join JobUser by jobId, JobTime by jobId, distinctJobStatus by jobId, mapTaskTime by jobId, reduceTaskTime by jobId;

-- JoinedJobTask = join JobUser by $0.jobId, JobTime by jobId, distinctJobStatus by jobId, mapReduceTaskTime by jobId;
userJobRecords = foreach JoinedJobTask generate JobUser::user as user, JobTime::jobId as jobId, JobTime::submitTime as submitTime, JobTime::finishTime as finishTime, JobTime::dataLocalMaps as dataLocalMaps, JobTime::rackLocalMaps as rackLocalMaps, JobTime::remoteMaps as remoteMaps, JobTime::totalMaps as totalMaps, JobTime::totalReduces as totalReduces, JobTime::mapInputBytes as mapInputBytes, JobTime::reduceOutputRecords as reduceOutputRecords, distinctJobStatus::jobStatus as jobStatus, mapTaskTime::slotHours as mapSlotHours, reduceTaskTime::slotHours as reduceSlotHours;

-- group by user
userGroup = group userJobRecords by user;
userSummary = foreach userGroup generate '$TIMESTAMP' as ts, group as user, COUNT(userJobRecords.jobId) as totalJobs, SUM(userJobRecords.dataLocalMaps) as dataLocalMaps, SUM(userJobRecords.rackLocalMaps) as rackLocalMaps, SUM(userJobRecords.remoteMaps) as remoteMaps, SUM(userJobRecords.mapInputBytes) as mapInputBytes, SUM(userJobRecords.reduceOutputRecords) as reduceOutputRecords, SUM(userJobRecords.mapSlotHours) as mapSlotHours, SUM(userJobRecords.reduceSlotHours) as reduceSlotHours, SUM(userJobRecords.totalMaps) as totalMaps, SUM(userJobRecords.totalReduces) as totalReduces, 'UserDailySummary' as c_recordtype, 'all' as c_source, 'UserSummaryPigScript' as c_application, '$cluster' as c_cluster;

describe userSummary;
-- dump userSummary;
store userSummary into '$output' using seqWriter;

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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class JobLog extends AbstractProcessor {
  private String savedLines = "";
  
  /**
   * Job logs could be split into multiple lines.  
   * If input recordEntry ends with '"' or '" .', process the line. 
   * Otherwise, save the log and wait for the next log. 
   * 
   * @return An object of JobLogLine if a full job log is found. Null otherwise.
   */
  public JobLogLine getJobLogLine(String recordEntry) {
    if(recordEntry == null) {
      savedLines = "";
      return null;
    }
    recordEntry = recordEntry.trim();
    if(recordEntry.length() == 0) {
      savedLines = "";
      return null;
    }
    
    if(recordEntry.startsWith("Job") 
        || recordEntry.startsWith("Meta") 
        || recordEntry.startsWith("Task") 
        || recordEntry.startsWith("MapAttempt") 
        || recordEntry.startsWith("ReduceAttempt")) 
    {
      savedLines = "";
    }
    
    savedLines += recordEntry;
    if(!savedLines.endsWith("\"") && !savedLines.endsWith("\" .")) {
      return null;
    }
    
    JobLogLine line = new JobLogLine(savedLines);
    return line;
  }
  
	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter) throws Throwable 
	{
	  JobLogLine line = getJobLogLine(recordEntry);
	  if(line == null || (!line.getLogType().equals("Meta") 
	                      && !line.getLogType().equals("JobData") 
	                      && !line.getLogType().equals("TaskData"))) 
	  {
	    return;
	  }
	  
    if(line.getLogType().equals("Meta")) {
      String streamName = chunk.getStreamName();
      if(streamName == null) {
        return;
      }
      String jobId = JobLogFileName.getJobIdFromFileName(streamName);
      if(jobId == null) {
        return;
      }
      line.setLogType("JobData");
    }
    
		key = new ChukwaRecordKey();
		ChukwaRecord record = new ChukwaRecord();
		this.buildGenericRecord(record, null, -1l, line.getLogType());
		
		for (Entry<String, String> entry : line.entrySet()) {
			record.add(entry.getKey(), entry.getValue());
		}
		
		for(Entry<String, Long> entry : line.getCounterHash().flat().entrySet()) {
			record.add(entry.getKey(), entry.getValue().toString());
		}
		
		long timestamp = line.getTimestamp();
		record.setTime(timestamp);
		key.setKey(getKey(timestamp, line.getJobId()));
		output.collect(key, record);
	}
	
	private String getKey(long ts, String jobId) {
		long unit = 60 * 60 * 1000;
		if(ts == 0) {
		  ts = archiveKey.getTimePartition();
		}
		long rounded = (ts / unit) * unit;
		return rounded + "/" + jobId + "/" + ts;
	}

	public static class JobLogLine extends HashMap<String, String> {
		private static final long serialVersionUID = 4902948603527677036L;
		
		/**
		 * search timestamp from stream. if no timestamp found, use last seen one.
		 */
		private static final String[] timestampKeys = { 
			JobHistory.Keys.SUBMIT_TIME.toString(),
			JobHistory.Keys.LAUNCH_TIME.toString(),
			JobHistory.Keys.START_TIME.toString(),
			JobHistory.Keys.FINISH_TIME.toString(), 
		};
		private static long lastTimestamp = 0l;

		private String logType;
		private String jobId;
		private String taskId;
		private CounterHash counterHash;

		/**
		 * example lines: 
		 * 		Task TASKID="task_200903062215_0577_r_000000" TASK_TYPE="REDUCE" START_TIME="1236386538540" SPLITS="" .
		 *		Job JOBID="job_200903062215_0577" JOB_PRIORITY="NORMAL" .
		 *		Job JOBID="job_200903062215_0577" LAUNCH_TIME="1236386526545" TOTAL_MAPS="14" TOTAL_REDUCES="1" JOB_STATUS="PREP" .
		 */
		public JobLogLine(String line) {
			line = line.trim();
			if (line.length() == 0)
				return;

			String key = null;
			String[] pairs = line.split("=\"");
			for (int i = 0; i < pairs.length; i++) {
				if (i == 0) {
					String[] fields = pairs[i].split(" ");
					
					logType = fields[0];
					if(logType.equals("Job")) {
						logType = "JobData";
					}
					else if (logType.equals("Task") || logType.equals("MapAttempt") || logType.equals("ReduceAttempt")) {
						logType = "TaskData";
					}
					
					if (fields.length > 1)
						key = fields[1];
					continue;
				}

				int pos = pairs[i].lastIndexOf('"');
				String value = pairs[i].substring(0, pos);
	                        put(key, value);				  
				if(i == (pairs.length-1))
					break;
				key = pairs[i].substring(pos + 2);
			}
			
			// jobid format: job_200903062215_0577
			jobId = get(JobHistory.Keys.JOBID.toString());
			
			// taskid format: task_200903062215_0577_r_000000
			taskId = get(JobHistory.Keys.TASKID.toString());
			if(taskId != null) {
				String[] fields = taskId.split("_");
				jobId = "job_" + fields[1] + "_" + fields[2];
				put(JobHistory.Keys.JOBID.toString(), jobId);
				taskId = taskId.substring(5);
			}
			
			counterHash = new CounterHash(get(JobHistory.Keys.COUNTERS.toString()));
			
			if(get("TASK_ATTEMPT_ID") != null) {
				put("TASK_ATTEMPT_TIMES", "" + getAttempts());
			}
			
			if(logType.equals("JobData") && get(JobHistory.Keys.FINISH_TIME.toString())!=null) {
			  put("JOB_FINAL_STATUS", get("JOB_STATUS"));
			}
			
            for(String timeKey : timestampKeys) {
                String value = get(timeKey);
                if(value == null || value.equals("0")) {
                    remove(timeKey);
                }
            }
		}

    public String getLogType() {
      return logType;
    }

    public void setLogType(String logType) {
      this.logType = logType;
    }

		public String getJobId() {
			return jobId;
		}

		public String getTaskId() {
			return taskId;
		}
		
		public long getTimestamp() {
			for(String key : timestampKeys) {
				String value = get(key);
				if(value != null && value.length() != 0) {
					long ts = Long.parseLong(value);
					if(ts > lastTimestamp) {
						lastTimestamp = ts;
					}
					break;
				}
			}
			return lastTimestamp;
		}
		
		public CounterHash getCounterHash() {
			return counterHash;
		}
		
		public int getAttempts() {
			String attemptId = get("TASK_ATTEMPT_ID");
			if(attemptId == null) {
				return -1;
			}
			else {
				try {
					String[] elems = attemptId.split("_");
					return Integer.parseInt(elems[elems.length - 1] + 1);
				} catch (NumberFormatException e) {
					return -1;
				}
			}
		}
	}
	
	/**
	 * Parse counter string to object
	 * 
	 * Example string:
	 * {(org\.apache\.hadoop\.mapred\.JobInProgress$Counter)(Job Counters )
		    [(TOTAL_LAUNCHED_REDUCES)(Launched reduce tasks)(1)]
		    [(TOTAL_LAUNCHED_MAPS)(Launched map tasks)(14)]
		    [(DATA_LOCAL_MAPS)(Data-local map tasks)(14)]
		}
		{(FileSystemCounters)(FileSystemCounters)
		    [(FILE_BYTES_READ)(FILE_BYTES_READ)(132)]
		    [(HDFS_BYTES_READ)(HDFS_BYTES_READ)(20471)]
		    [(FILE_BYTES_WRITTEN)(FILE_BYTES_WRITTEN)(790)]
		    [(HDFS_BYTES_WRITTEN)(HDFS_BYTES_WRITTEN)(248)]
		}
	 */
	public static class CounterHash extends HashMap<String, HashMap<String, Long>>{
		public CounterHash(String str) {
			if(str == null) {
				return;
			}
			
			if(str.startsWith("{")) {
				for(String group : split(str, "[{}]")) {
					HashMap<String, Long> hash = null; 
					for(String counter : split(group, "[\\[\\]]")) {
						ArrayList<String> idAndDisplay = split(counter, "[\\(\\)]");
						if(hash == null) {
							hash = new HashMap<String, Long>();
							String groupId = idAndDisplay.get(0).replaceAll("\\\\.", ".");
							put(groupId, hash);
						}
						else {
							hash.put(idAndDisplay.get(0), Long.parseLong(idAndDisplay.get(2)));
						}
					}
				}
			} else {
				HashMap<String, Long> hash = new HashMap<String, Long>();
				put("Hadoop18", hash);
				for(String counter : split(str, ",")) {
					ArrayList<String> kv = split(counter, ":");
					hash.put(kv.get(0), Long.parseLong(kv.get(1)));
				}
			}
		}
		
		/**
		 * Flat the counter hashs and add into map passed int. 
		 * 
		 * For example mentioned in the constructor, the result will be
		 * <pre>
		 * Counter:org\.apache\.hadoop\.mapred\.JobInProgress$Counter:TOTAL_LAUNCHED_REDUCES=1
		 * Counter:org\.apache\.hadoop\.mapred\.JobInProgress$Counter:TOTAL_LAUNCHED_MAPS=14
		 * Counter:org\.apache\.hadoop\.mapred\.JobInProgress$Counter:DATA_LOCAL_MAPS=14
		 * Counter:FileSystemCounters:FILE_BYTES_READ=132
		 * Counter:FileSystemCounters:HDFS_BYTES_READ=20471
		 * Counter:FileSystemCounters:FILE_BYTES_WRITTEN=790
		 * Counter:FileSystemCounters:HDFS_BYTES_WRITTEN=248
		 * </pre>
		 */
		public HashMap<String, Long> flat() {
			HashMap<String, Long> result = new HashMap<String, Long>();
			for(Entry<String, HashMap<String, Long>> entry : entrySet()) {
				String id = entry.getKey();
				for(Entry<String, Long> counterValue : entry.getValue().entrySet()) {
					result.put("Counter:" + id + ":" + counterValue.getKey(), counterValue.getValue());
				}
			}
			return result;
		}
	}
	
	public static ArrayList<String> split(String s, String regex) {
		ArrayList<String> result = new ArrayList<String>();
		for(String field : s.split(regex)) {
			if(field != null && field.length()>0) {
				result.add(field);
			}
		}
		return result;
	}

	private static class JobLogFileName {
    private static final Pattern pattern = Pattern.compile("job_[0-9]+_[0-9]+");
    
    public static String getJobIdFromFileName(String name) {
      Matcher matcher = pattern.matcher(name);
      if (matcher.find()) {
        return matcher.group(0);
      }
      else {
        return null;
      }
    }
	}

}

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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.JobLog.JobLogLine;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import junit.framework.TestCase;

public class TestJobLogEntry extends TestCase {
	private ArrayList<String> testLogList = new ArrayList<String>();

	protected void setUp() throws Exception {
		super.setUp();
		InputStream stream = this.getClass().getResourceAsStream("/TestJobLog.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(stream));
		while(true) {
			String line = br.readLine();
			if(line == null)
				break;
			testLogList.add(line);
		}
		
		stream = this.getClass().getResourceAsStream("/Hadoop18JobHistoryLog.txt");
		br = new BufferedReader(new InputStreamReader(stream));
		while(true) {
			String line = br.readLine();
			if(line == null)
				break;
			testLogList.add(line);
		}
	}

	public void testJobLogEntry() {
    JobLog jobLog = new JobLog();
		JobLogLine log = jobLog.getJobLogLine(testLogList.get(1));
		assertEquals("JobData", log.getLogType());
		assertEquals("hdfs://test33/tmp/hadoop-gmon/mapred/system/job_200903062215_0577/job\\.xml", log.get("JOBCONF"));
		assertEquals("job_200903062215_0577", log.get("JOBID"));
		assertEquals("grep-search", log.get("JOBNAME"));
		assertEquals("gmon", log.get("USER"));
		assertEquals("1236386525570", log.get("SUBMIT_TIME"));
		assertEquals(1236386525570l, log.getTimestamp());
		
		log = jobLog.getJobLogLine(testLogList.get(2));
		assertEquals(1236386525570l, log.getTimestamp());
		
		log = jobLog.getJobLogLine(testLogList.get(4));
		assertEquals("TaskData", log.getLogType());
		assertEquals("", log.get("SPLITS"));
		assertEquals(1236386529449l, log.getTimestamp());
		
		log = jobLog.getJobLogLine(testLogList.get(72));
		assertEquals("TaskData", log.getLogType());
		assertEquals("{(org\\.apache\\.hadoop\\.mapred\\.Task$Counter)(Map-Reduce Framework)[(REDUCE_INPUT_GROUPS)(Reduce input groups)(0)][(COMBINE_OUTPUT_RECORDS)(Combine output records)(0)][(REDUCE_SHUFFLE_BYTES)(Reduce shuffle bytes)(0)][(REDUCE_OUTPUT_RECORDS)(Reduce output records)(0)][(SPILLED_RECORDS)(Spilled Records)(0)][(COMBINE_INPUT_RECORDS)(Combine input records)(0)][(REDUCE_INPUT_RECORDS)(Reduce input records)(0)]}", log.get("COUNTERS"));
		
		log = jobLog.getJobLogLine(testLogList.get(73));
		HashMap<String, Long> counters = log.getCounterHash().flat();
		assertEquals("1", counters.get("Counter:org.apache.hadoop.mapred.JobInProgress$Counter:TOTAL_LAUNCHED_REDUCES").toString());
		assertEquals("20471", counters.get("Counter:FileSystemCounters:HDFS_BYTES_READ").toString());
		
		log = jobLog.getJobLogLine(testLogList.get(90));
		assertTrue("START_TIME should not exist", log.get("START_TIME")==null);

		log = jobLog.getJobLogLine("");
		assertTrue(log==null);
		
		log = jobLog.getJobLogLine("Job JOBID=\"job_200903042324_8630\" FINISH_TIME=\"1236527538594\" JOB_STATUS=\"SUCCESS\" FINISHED_MAPS=\"10\" FINISHED_REDUCES=\"8\" FAILED_MAPS=\"0\" FAILED_REDUCES=\"0\" COUNTERS=\"input records:0,Map-Reduce Framework.Reduce input records:57038\"");
		
		// print all key-values
		for(String line : testLogList) {
			log = jobLog.getJobLogLine(line);
			if(log == null) {
			  continue;
			}
			System.out.println(log.getLogType());
			for(Entry<String, String> entry : log.entrySet()) {
				String k = entry.getKey();
				String v = entry.getValue();
				System.out.println(k + ": " + v);
				if(k.equals("START_TIME") || k.equals("FINISH_TIME"))
					assertTrue(v!=null && !v.equals("0"));
			}
			
			// list all counters for this entry
			for(Entry<String, Long> entry : log.getCounterHash().flat().entrySet()) {
				System.out.println(entry.getKey() + ": " + entry.getValue());
			}
			
			System.out.println();
		}
	}
	
}

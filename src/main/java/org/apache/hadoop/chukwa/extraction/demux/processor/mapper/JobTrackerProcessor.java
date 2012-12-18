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

import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;


@Tables(annotations={
@Table(name="JobTracker",columnFamily="jt"),
@Table(name="JobTracker",columnFamily="jvm"),
@Table(name="JobTracker",columnFamily="rpc")
})
public class JobTrackerProcessor extends AbstractProcessor{
	static Map<String, Long> rateMap = new ConcurrentHashMap<String,Long>();
	static {
		long zero = 0L;	
		rateMap.put("SentBytes", zero);
		rateMap.put("ReceivedBytes", zero);
		rateMap.put("rpcAuthorizationSuccesses", zero);
		rateMap.put("rpcAuthorizationFailures", zero);
		rateMap.put("RpcQueueTime_num_ops", zero);
		rateMap.put("RpcProcessingTime_num_ops", zero);
		rateMap.put("heartbeats", zero);
		rateMap.put("jobs_submitted", zero);
		rateMap.put("jobs_completed", zero);
		rateMap.put("jobs_failed", zero);
		rateMap.put("jobs_killed", zero);
		rateMap.put("maps_launched", zero);
		rateMap.put("maps_completed", zero);
		rateMap.put("maps_failed", zero);
		rateMap.put("maps_killed", zero);
		rateMap.put("reduces_launched", zero);
		rateMap.put("reduces_completed", zero);
		rateMap.put("reduces_failed", zero);
		rateMap.put("reduces_killed", zero);
		rateMap.put("gcCount", zero);
	}

	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter) throws Throwable {
		Logger log = Logger.getLogger(JobTrackerProcessor.class); 
		long timeStamp = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();
		
		final ChukwaRecord mapred_jt = new ChukwaRecord();
		final ChukwaRecord jt_jvm = new ChukwaRecord();
		final ChukwaRecord jt_rpc = new ChukwaRecord();
		
		Map<String, ChukwaRecord> metricsMap = new HashMap<String, ChukwaRecord>(){
			private static final long serialVersionUID = 1L;
			{
				put("gcCount", jt_jvm);
				put("gcTimeMillis", jt_jvm);
				put("logError", jt_jvm);
				put("logFatal", jt_jvm);
				put("logInfo", jt_jvm);
				put("logWarn", jt_jvm);
				put("memHeapCommittedM", jt_jvm);
				put("memHeapUsedM", jt_jvm);
				put("threadsBlocked", jt_jvm);
				put("threadsNew", jt_jvm);
				put("threadsRunnable", jt_jvm);
				put("threadsTerminated", jt_jvm);
				put("threadsTimedWaiting", jt_jvm);
				put("threadsWaiting", jt_jvm);

				put("ReceivedBytes", jt_rpc);				
				put("RpcProcessingTime_avg_time", jt_rpc);	
				put("RpcProcessingTime_num_ops", jt_rpc);	
				put("RpcQueueTime_avg_time", jt_rpc);	
				put("RpcQueueTime_num_ops", jt_rpc);	
				put("SentBytes", jt_rpc);	
				put("rpcAuthorizationSuccesses", jt_rpc);	
				put("rpcAuthorizationnFailures", jt_rpc);	
			}
		};		
		try{
			JSONObject obj = (JSONObject) JSONValue.parse(recordEntry);	
			String ttTag = chunk.getTag("timeStamp");
			if(ttTag == null){
				log.warn("timeStamp tag not set in JMX adaptor for jobtracker");
			}
			else{
				timeStamp = Long.parseLong(ttTag);
			}
			Iterator<JSONObject> iter = obj.entrySet().iterator();
			
			while(iter.hasNext()){
				Map.Entry entry = (Map.Entry)iter.next();
				String key = (String) entry.getKey();
				Object value = entry.getValue();
				String valueString = value == null?"":value.toString();	
				
				//Calculate rate for some of the metrics
				if(rateMap.containsKey(key)){
					long oldValue = rateMap.get(key);
					long curValue = Long.parseLong(valueString);
					rateMap.put(key, curValue);
					long newValue = curValue - oldValue;
					if(newValue < 0){
						log.warn("JobTrackerProcessor's rateMap might be reset or corrupted for metric "+key);						
						newValue = 0L;
					}					
					valueString = Long.toString(newValue);
				}
				
				//These metrics are string types with JSON structure. So we parse them and get the count
				if(key.indexOf("Json") >= 0){	
					//ignore these for now. Parsing of JSON array is throwing class cast exception. 
				}	
				else if(metricsMap.containsKey(key)){
					ChukwaRecord rec = metricsMap.get(key);
					rec.add(key, valueString);
				}
				else {
					mapred_jt.add(key, valueString);
				}
			}			
			
			buildGenericRecord(mapred_jt, null, timeStamp, "jt");
			output.collect(key, mapred_jt);
			buildGenericRecord(jt_jvm, null, timeStamp, "jvm");
			output.collect(key, jt_jvm);
			buildGenericRecord(jt_rpc, null, timeStamp, "rpc");
			output.collect(key, jt_rpc);
		}
		catch(Exception e){
			log.error(ExceptionUtil.getStackTrace(e));
		}
	}
}


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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.Buffer;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

@Tables(annotations={
@Table(name="HBase",columnFamily="master")
})
public class HBaseMasterProcessor extends AbstractProcessor{
	static Map<String, Long> rateMap = new ConcurrentHashMap<String,Long>();
	static {
		long zero = 0L;	
		rateMap.put("splitSizeNumOps", zero);
		rateMap.put("splitTimeNumOps", zero);
	}
	
	@Override
	protected void parse(String recordEntry,
			OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
			Reporter reporter) throws Throwable {
		
		Logger log = Logger.getLogger(HBaseMasterProcessor.class); 
		long timeStamp = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();
		ChukwaRecord record = new ChukwaRecord();
		
		Map<String, Buffer> metricsMap = new HashMap<String,Buffer>();

		try{
			JSONObject obj = (JSONObject) JSONValue.parse(recordEntry);	
			String ttTag = chunk.getTag("timeStamp");
			if(ttTag == null){
				log.warn("timeStamp tag not set in JMX adaptor for hbase master");
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
						log.warn("HBaseMaster rateMap might be reset or corrupted for metric "+key);						
						newValue = 0L;
					}					
					valueString = Long.toString(newValue);
				}
				
				Buffer b = new Buffer(valueString.getBytes());
				metricsMap.put(key,b);				
			}			
			
			TreeMap<String, Buffer> t = new TreeMap<String, Buffer>(metricsMap);
			record.setMapFields(t);			
			buildGenericRecord(record, null, timeStamp, "master");
			output.collect(key, record);
		}
		catch(Exception e){
			log.error(ExceptionUtil.getStackTrace(e));
		}		
	}	
}

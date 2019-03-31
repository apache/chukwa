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

package org.apache.hadoop.chukwa.analysis.salsa.fsm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.*;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.chukwa.extraction.demux.*;
import org.apache.hadoop.chukwa.extraction.engine.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * Pluggable mapper for FSMBuilder
 *
 * K2 = State Name + State ID 
 * (We use ChukwaRecordKey since it would already have implemented a bunch of
 *  useful things such as Comparators etc.)
 * V2 = TreeMap
 */
public class TaskTrackerClientTraceMapper 
  extends MapReduceBase 
  implements Mapper<ChukwaRecordKey, ChukwaRecord, ChukwaRecordKey, FSMIntermedEntry> 
{
  private static Log log = LogFactory.getLog(FSMBuilder.class);
	protected static final String SEP = "/";
	protected final static String FSM_CRK_ReduceType = FSMType.NAMES[FSMType.MAPREDUCE_FSM];
	private final Pattern ipPattern =
    Pattern.compile("([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)[a-zA-Z\\-_:\\/].*");
  
  public void map
    (ChukwaRecordKey key, ChukwaRecord val,
     OutputCollector<ChukwaRecordKey, FSMIntermedEntry> output, 
		 Reporter reporter)
    throws IOException 
  {

		/* Extract field names for checking */
		String [] fieldNames = val.getFields();
		ArrayList<String> fieldNamesList = new ArrayList<String>(fieldNames.length);
		for (int i = 0; i < fieldNames.length; i++) {
		  fieldNamesList.add(fieldNames[i]);
		}
		
		// Handle ClientTraceDetailed and DataNodeLog entries separately
		// because we need to combine both types of entries for a complete picture
		
		if (key.getReduceType().equals("ClientTraceDetailed")) {
		  assert(fieldNamesList.contains("op"));
		  if (val.getValue("op").startsWith("MAPRED")) { 
		    parseClientTraceDetailed(key, val, output, reporter, fieldNamesList);
	    } // pick up only mapreduce operations
		} 
  } // end of map()

  protected final int DEFAULT_SHUFFLE_DURATION_MS = 10;

  // works with 0.20 ClientTrace with no durations
  // includes hack to create start+end entries immediately
  protected void parseClientTraceDetailed
    (ChukwaRecordKey key, ChukwaRecord val,
     OutputCollector<ChukwaRecordKey, FSMIntermedEntry> output, 
		 Reporter reporter, ArrayList<String> fieldNamesList)
    throws IOException
  {
    FSMIntermedEntry start_rec, end_rec;
    String current_op = null, src_add = null, dest_add = null;
    String reduce_id = null, map_id = null;
    
    /* initialize state records */
    start_rec = new FSMIntermedEntry();
    end_rec = new FSMIntermedEntry();
    start_rec.fsm_type = new FSMType(FSMType.MAPREDUCE_FSM);
    start_rec.state_type = new StateType(StateType.STATE_START);
    end_rec.fsm_type = new FSMType(FSMType.MAPREDUCE_FSM);
    end_rec.state_type = new StateType(StateType.STATE_END);    
        
    /* extract addresses */
    Matcher src_regex = ipPattern.matcher(val.getValue("src"));
    if (src_regex.matches()) {
      src_add = src_regex.group(1);
    } else {
      log.warn("Failed to match src IP:"+val.getValue("src")+"");
      src_add = "";
    }
    Matcher dest_regex = ipPattern.matcher(val.getValue("dest"));
    if (dest_regex.matches()) {
      dest_add = dest_regex.group(1);
    } else {
      log.warn("Failed to match dest IP:"+val.getValue("dest")+"");
      dest_add = "";
    }
    if (fieldNamesList.contains("reduceID")) {
      reduce_id = val.getValue("reduceID");
    } else {
      // add a random number so we get unique keys or the CRK will break
      Random r = new Random(); 
      reduce_id = "noreduce" + r.nextInt();
    }
    
    if (fieldNamesList.contains("cliID")) {
      map_id = val.getValue("cliID").trim();
    } else {
      map_id = "nomap";
    }
    
    current_op = val.getValue("op");
    
    start_rec.host_exec = src_add;
    end_rec.host_exec = src_add;
    start_rec.host_other = dest_add;
    end_rec.host_other = dest_add;
            
    // timestamp of the log entry is the end time; 
    // subtract duration to get start time
    long actual_time_ms = Long.parseLong(val.getValue("actual_time"));
    if (fieldNamesList.contains("duration")) {
      try {
        actual_time_ms -= (Long.parseLong(val.getValue("duration").trim()) / 1000);
      } catch (NumberFormatException nef) {
        log.warn("Failed to parse duration: >>" + val.getValue("duration"));
      }
    } else {
      actual_time_ms -= DEFAULT_SHUFFLE_DURATION_MS;
    }
    
    String [] k = key.getKey().split("/");    

    start_rec.time_orig_epoch = k[0];
    start_rec.time_orig = Long.toString(actual_time_ms); // not actually used
    start_rec.timestamp = Long.toString(actual_time_ms);
    start_rec.time_end = "";
    start_rec.time_start = start_rec.timestamp;
    
    end_rec.time_orig_epoch = k[0];
    end_rec.time_orig = val.getValue("actual_time");
    end_rec.timestamp = val.getValue("actual_time");
    end_rec.time_end = val.getValue("actual_time");
    end_rec.time_start = "";

    log.debug("Duration: " + (Long.parseLong(end_rec.time_end) - Long.parseLong(start_rec.time_start)));

    start_rec.job_id = reduce_id; // use job id = block id
    end_rec.job_id = reduce_id; 
          
    if (current_op.equals("MAPRED_SHUFFLE")) {
      if (src_add != null && src_add.equals(dest_add)) {
        start_rec.state_mapred = new MapRedState(MapRedState.SHUFFLE_LOCAL);
      } else {
        start_rec.state_mapred = new MapRedState(MapRedState.SHUFFLE_REMOTE);
      }
    } else {
      log.warn("Invalid state: " + current_op);
    }
    end_rec.state_mapred = start_rec.state_mapred;
    start_rec.state_name = start_rec.state_mapred.toString();
    end_rec.state_name = end_rec.state_mapred.toString();
    start_rec.identifier = new StringBuilder().append(reduce_id).append("@").append(map_id).toString();
    end_rec.identifier = new StringBuilder().append(reduce_id).append("@").append(map_id).toString();
    
    start_rec.generateUniqueID();
    end_rec.generateUniqueID();
        
    start_rec.add_info.put(Record.tagsField,val.getValue(Record.tagsField));
		start_rec.add_info.put("csource",val.getValue("csource"));
    end_rec.add_info.put(Record.tagsField,val.getValue(Record.tagsField));
		end_rec.add_info.put("csource",val.getValue("csource"));
		end_rec.add_info.put("STATE_STRING","SUCCESS"); // by default
		
		// add counter value
		end_rec.add_info.put("BYTES",val.getValue("bytes"));
		    
    String crk_mid_string_start = new StringBuilder().append(start_rec.getUniqueID()).append("_").append(start_rec.timestamp).toString();
    String crk_mid_string_end = new StringBuilder().append(end_rec.getUniqueID()).append("_").append(start_rec.timestamp).toString();
    output.collect(new ChukwaRecordKey(FSM_CRK_ReduceType, crk_mid_string_start), start_rec);
    output.collect(new ChukwaRecordKey(FSM_CRK_ReduceType, crk_mid_string_end), end_rec);
    
  }

} // end of mapper class

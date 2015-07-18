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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.chukwa.extraction.engine.*;
import org.apache.hadoop.mapred.*;

/**
 * Pluggable mapper for FSMBuilder
 * Supports only 0.20+ JobHistory files
 * because of explicitly coded counter names
 *
 * K2 = State Name + State ID 
 * (We use ChukwaRecordKey since it would already have implemented a bunch of
 *  useful things such as Comparators etc.)
 * V2 = TreeMap
 */
public class JobHistoryTaskDataMapper 
  extends MapReduceBase 
  implements Mapper<ChukwaRecordKey, ChukwaRecord, ChukwaRecordKey, FSMIntermedEntry> 
{
  private static Log log = LogFactory.getLog(FSMBuilder.class);
	protected static final String SEP = "/";
	
	protected final static String FSM_CRK_ReduceType = FSMType.NAMES[FSMType.MAPREDUCE_FSM];

	/*
	 * Helper function for mapper to populate TreeMap of FSMIntermedEntr
	 * with input/output counters for Map records
	 */
	protected FSMIntermedEntry populateRecord_MapCounters
		(FSMIntermedEntry this_rec, ChukwaRecord val, ArrayList<String> fieldNamesList) 
	{
		String mapCounterNames [] = {		
			"Counter:FileSystemCounters:FILE_BYTES_WRITTEN",
			"Counter:org.apache.hadoop.mapred.Task$Counter:COMBINE_INPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:COMBINE_OUTPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:MAP_INPUT_BYTES",
			"Counter:org.apache.hadoop.mapred.Task$Counter:MAP_INPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:MAP_OUTPUT_BYTES",
			"Counter:org.apache.hadoop.mapred.Task$Counter:MAP_OUTPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:SPILLED_RECORDS"
		};
		String mapCounterDestNames[] = {
			"FILE_BYTES_WRITTEN",
			"COMBINE_INPUT_RECORDS",
			"COMBINE_OUTPUT_RECORDS",
			"INPUT_BYTES",
			"INPUT_RECORDS",
			"OUTPUT_BYTES",
			"OUTPUT_RECORDS",
			"SPILLED_RECORDS"
		};
		
		assert(mapCounterDestNames.length == mapCounterNames.length);
		
		for (int i = 0; i < mapCounterDestNames.length; i++) {
			if (fieldNamesList.contains(mapCounterNames[i])) {
				this_rec.add_info.put(mapCounterDestNames[i], val.getValue(mapCounterNames[i]));				
			}
		}
		this_rec.add_info.put("FILE_BYTES_READ","0"); // to have same fields as reduce
		this_rec.add_info.put("INPUT_GROUPS","0"); // to have same fields as reduce
		
		return this_rec;
	}

	/*
	 * Helper function for mapper to populate TreeMap of FSMIntermedEntr
	 * with input/output counters for Reduce records
	 */
	protected FSMIntermedEntry populateRecord_ReduceCounters
		(FSMIntermedEntry this_rec, ChukwaRecord val, ArrayList<String> fieldNamesList) 
	{
		String redCounterNames [] = {		
			"Counter:FileSystemCounters:FILE_BYTES_READ",
			"Counter:FileSystemCounters:FILE_BYTES_WRITTEN",
			"Counter:org.apache.hadoop.mapred.Task$Counter:COMBINE_INPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:COMBINE_OUTPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:REDUCE_INPUT_GROUPS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:REDUCE_INPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:REDUCE_OUTPUT_RECORDS",
			"Counter:org.apache.hadoop.mapred.Task$Counter:REDUCE_SHUFFLE_BYTES",
			"Counter:org.apache.hadoop.mapred.Task$Counter:SPILLED_RECORDS"
		};
		String redCounterDestNames[] = {
			"FILE_BYTES_READ",
			"FILE_BYTES_WRITTEN",
			"COMBINE_INPUT_RECORDS",
			"COMBINE_OUTPUT_RECORDS",
			"INPUT_GROUPS",
			"INPUT_RECORDS",
			"OUTPUT_RECORDS",
			"INPUT_BYTES", // NOTE: shuffle bytes are mapped to "input_bytes"
			"SPILLED_RECORDS"
		};
		
		assert(redCounterDestNames.length == redCounterNames.length);
		
		for (int i = 0; i < redCounterDestNames.length; i++) {
			if (fieldNamesList.contains(redCounterNames[i])) {
				this_rec.add_info.put(redCounterDestNames[i], val.getValue(redCounterNames[i]));				
			}
		}
		
		this_rec.add_info.put("OUTPUT_BYTES","0"); // to have same fields as map		
		
		return this_rec;		
	}

  public void map
    (ChukwaRecordKey key, ChukwaRecord val,
     OutputCollector<ChukwaRecordKey, FSMIntermedEntry> output, 
		 Reporter reporter)
    throws IOException 
  {
		String task_type;
		FSMIntermedEntry this_rec = new FSMIntermedEntry(); 
		boolean add_record = true;

		/* Extract field names for checking */
		String [] fieldNames = val.getFields();
		ArrayList<String> fieldNamesList = new ArrayList<String>(fieldNames.length);
		for (int i = 0; i < fieldNames.length; i++) fieldNamesList.add(fieldNames[i]); 
					
		/* Check state (Map or Reduce), generate unique ID */
		if (!fieldNamesList.contains("TASK_ATTEMPT_ID")) return; // Ignore "TASK" entries
		if (!fieldNamesList.contains("TASK_TYPE")) { // Malformed, ignore
			return;
		} else {
			task_type = val.getValue("TASK_TYPE"); 
			if (!task_type.equals("MAP") && !task_type.equals("REDUCE")) {
				return; // do nothing
			} 
		} 

		/* Check if this is a start or end entry, set state type, extract start/end times */
		if (fieldNamesList.contains("START_TIME")) {
			this_rec.state_type.val = StateType.STATE_START;
			this_rec.timestamp = val.getValue("START_TIME");
			this_rec.time_start = val.getValue("START_TIME");
			this_rec.time_end = "";
			if (val.getValue("START_TIME").length() < 4+2) { // needs to at least have milliseconds
			  add_record = add_record & false;
			} 
		} else if (fieldNamesList.contains("FINISH_TIME")) {
			this_rec.state_type.val = StateType.STATE_END;
			this_rec.timestamp = val.getValue("FINISH_TIME");
			this_rec.time_start = "";
			this_rec.time_end = val.getValue("FINISH_TIME");
			if (val.getValue("FINISH_TIME").length() < 4+2) { // needs to at least have milliseconds
			  add_record = add_record & false;
			} 		
		} else {
			this_rec.state_type.val = StateType.STATE_NOOP;
		}
		
		/* Fill in common intermediate state entry information */
		
		// Extract original ChukwaRecordKey values for later key reconstruction by reducer
		try {
			this_rec = ParseUtilities.splitChukwaRecordKey(key.getKey().trim(),this_rec,SEP);
		} catch (Exception e) {
			log.warn("Error occurred splitting ChukwaRecordKey ["+key.getKey().trim()+"]: " + e.toString());
			return;
		}
		
		// Populate state enum information
		this_rec.fsm_type = new FSMType(FSMType.MAPREDUCE_FSM);
		if (task_type.equals("MAP")) {
			this_rec.state_mapred = new MapRedState(MapRedState.MAP);
		} else if (task_type.equals("REDUCE")) {
			this_rec.state_mapred = new MapRedState(MapRedState.REDUCE);
		} else {
			this_rec.state_mapred = new MapRedState(MapRedState.NONE); // error handling here?
		}
		
		// Fill state name, unique ID
		this_rec.state_name = this_rec.state_mapred.toString();
		this_rec.identifier = val.getValue("TASK_ATTEMPT_ID");
		this_rec.generateUniqueID();
		
		// Extract hostname from tracker name (if present), or directly fill from hostname (<= 0.18)
		if (fieldNamesList.contains("HOSTNAME")) {
			this_rec.host_exec = val.getValue("HOSTNAME");
			this_rec.host_exec = ParseUtilities.removeRackFromHostname(this_rec.host_exec);
		} else if (fieldNamesList.contains("TRACKER_NAME")) {
			this_rec.host_exec = ParseUtilities.extractHostnameFromTrackerName(val.getValue("TRACKER_NAME"));
		} else {
			this_rec.host_exec = "";
		}
		
		if (this_rec.state_type.val == StateType.STATE_END) {
			assert(fieldNamesList.contains("TASK_STATUS"));
			String tmpstring = null;
			tmpstring = val.getValue("TASK_STATUS");
			if (tmpstring != null && (tmpstring.equals("KILLED") || tmpstring.equals("FAILED"))) {
			  add_record = add_record & false;
			}
			if (tmpstring != null && tmpstring.length() > 0) {
				this_rec.add_info.put("STATE_STRING",tmpstring);
			} else {
				this_rec.add_info.put("STATE_STRING","");
			}
			
			switch(this_rec.state_mapred.val) {
				case MapRedState.MAP:
					this_rec = populateRecord_MapCounters(this_rec, val, fieldNamesList);
					break;
				case MapRedState.REDUCE:
					this_rec = populateRecord_ReduceCounters(this_rec, val, fieldNamesList);
					break;
				default:
					// do nothing
					break;
			}
		}
		// manually add clustername etc
		assert(fieldNamesList.contains(Record.tagsField));
		assert(fieldNamesList.contains("csource"));
		this_rec.add_info.put(Record.tagsField,val.getValue(Record.tagsField));
		this_rec.add_info.put("csource",val.getValue("csource"));
				
		/* Special handling for Reduce Ends */
		if (task_type.equals("REDUCE")) {
			if (this_rec.state_type.val == StateType.STATE_END) {
				add_record = add_record & expandReduceEnd(key,val,output,reporter,this_rec);
			} else if (this_rec.state_type.val == StateType.STATE_START) {
				add_record = add_record & expandReduceStart(key,val,output,reporter,this_rec);				
			}
		} else if (task_type.equals("MAP")) {
		  add_record = add_record & true;
		}
		
		if (add_record) {
		  log.debug("Collecting record " + this_rec + "("+this_rec.state_type+") (ReduceType "+FSM_CRK_ReduceType+")");
		  output.collect(new ChukwaRecordKey(FSM_CRK_ReduceType,this_rec.getUniqueID()),this_rec); 
	  }
		
  } // end of map()

	protected boolean expandReduceStart
			(ChukwaRecordKey key, ChukwaRecord val,
	   	 OutputCollector<ChukwaRecordKey, FSMIntermedEntry> output, 
		 	 Reporter reporter, FSMIntermedEntry this_rec)
			throws IOException
	{
		FSMIntermedEntry redshuf_start_rec = null;
		
		try {
			redshuf_start_rec = this_rec.clone();
		} catch (CloneNotSupportedException e) {
			// TODO: Error handling
		}
				
		redshuf_start_rec.state_type 		= new StateType(StateType.STATE_START);
		redshuf_start_rec.state_mapred 	= new MapRedState(MapRedState.REDUCE_SHUFFLEWAIT);
	 	
		redshuf_start_rec.timestamp = this_rec.timestamp;
		redshuf_start_rec.time_start = this_rec.timestamp;
		redshuf_start_rec.time_end = "";
		
		redshuf_start_rec.generateUniqueID();
		
		log.debug("Collecting record " + redshuf_start_rec + 
			"("+redshuf_start_rec.state_type+") (ReduceType "+FSM_CRK_ReduceType+")");
		output.collect(
			new ChukwaRecordKey(FSM_CRK_ReduceType,redshuf_start_rec.getUniqueID()),
			redshuf_start_rec
		);
		
		return true;
	}
	/*
	 * Generates 5 extra FSMIntermedEntry's for a given reduce_end entry
	 */
	protected boolean expandReduceEnd 
		(ChukwaRecordKey key, ChukwaRecord val,
   	 OutputCollector<ChukwaRecordKey, FSMIntermedEntry> output, 
	 	 Reporter reporter, FSMIntermedEntry this_rec)
		throws IOException
	{
		/* Split into ReduceShuffleWait, ReduceSort, ReduceReducer
		 * But also retain the original combined Reduce at the same time 
		 */
		FSMIntermedEntry redshuf_end_rec = null;
		FSMIntermedEntry redsort_start_rec = null, redsort_end_rec = null;
		FSMIntermedEntry redred_start_rec = null, redred_end_rec = null;
		
		/* Extract field names for checking */
		String [] fieldNames = val.getFields();
		ArrayList<String> fieldNamesList = new ArrayList<String>(fieldNames.length);
		for (int i = 0; i < fieldNames.length; i++) fieldNamesList.add(fieldNames[i]); 
		
		try {
		 	redsort_start_rec 	= this_rec.clone();
		 	redred_start_rec 		= this_rec.clone();
		 	redshuf_end_rec		= this_rec.clone();
		 	redsort_end_rec 	= this_rec.clone();
		 	redred_end_rec 		= this_rec.clone();
		} catch (CloneNotSupportedException e) {
			// TODO: Error handling
		}

		redshuf_end_rec.state_type 			= new StateType(StateType.STATE_END);
		redshuf_end_rec.state_mapred 		= new MapRedState(MapRedState.REDUCE_SHUFFLEWAIT);

		redsort_start_rec.state_type 		= new StateType(StateType.STATE_START);
		redsort_end_rec.state_type 			= new StateType(StateType.STATE_END);
		redsort_start_rec.state_mapred 	= new MapRedState(MapRedState.REDUCE_SORT);
		redsort_end_rec.state_mapred 		= new MapRedState(MapRedState.REDUCE_SORT);

		redred_start_rec.state_type 		= new StateType(StateType.STATE_START);
		redred_end_rec.state_type 			= new StateType(StateType.STATE_END);
		redred_start_rec.state_mapred		= new MapRedState(MapRedState.REDUCE_REDUCER);
		redred_end_rec.state_mapred			= new MapRedState(MapRedState.REDUCE_REDUCER);
		
		redshuf_end_rec.generateUniqueID();
		redsort_start_rec.generateUniqueID();
		redsort_end_rec.generateUniqueID();
		redred_start_rec.generateUniqueID();
		redred_end_rec.generateUniqueID();
		
		if(fieldNamesList.contains("SHUFFLE_FINISHED") && fieldNamesList.contains("SORT_FINISHED")) {
		  if (val.getValue("SHUFFLE_FINISHED") == null) return false;
		  if (val.getValue("SORT_FINISHED") == null) return false;
		} else {
		  return false;
		}
		redshuf_end_rec.timestamp = val.getValue("SHUFFLE_FINISHED");
		redshuf_end_rec.time_start = "";
		redshuf_end_rec.time_end = val.getValue("SHUFFLE_FINISHED");
		redsort_start_rec.timestamp = val.getValue("SHUFFLE_FINISHED"); 
		redsort_start_rec.time_start = val.getValue("SHUFFLE_FINISHED"); 
		redsort_start_rec.time_end = "";
		
		assert(fieldNamesList.contains("SORT_FINISHED"));
		redsort_end_rec.timestamp = val.getValue("SORT_FINISHED");
		redsort_end_rec.time_start = "";
		redsort_end_rec.time_end = val.getValue("SORT_FINISHED");
		redred_start_rec.timestamp = val.getValue("SORT_FINISHED");
		redred_start_rec.time_start = val.getValue("SORT_FINISHED");
		redred_start_rec.time_end = "";
		
		/* redred_end times are exactly the same as the original red_end times */
		
		log.debug("Collecting record " + redshuf_end_rec + 
			"("+redshuf_end_rec.state_type+") (ReduceType "+FSM_CRK_ReduceType+")");
		output.collect(
			new ChukwaRecordKey(FSM_CRK_ReduceType,redshuf_end_rec.getUniqueID()),
			redshuf_end_rec
		);
		
		log.debug("Collecting record " + redsort_start_rec + 
			"("+redsort_start_rec.state_type+") (ReduceType "+FSM_CRK_ReduceType+")");
		output.collect(
			new ChukwaRecordKey(FSM_CRK_ReduceType,redsort_start_rec.getUniqueID()),
			redsort_start_rec
		);
		
		log.debug("Collecting record " + redsort_end_rec + 
			"("+redsort_end_rec.state_type+") (ReduceType "+FSM_CRK_ReduceType+")");
		output.collect(
			new ChukwaRecordKey(FSM_CRK_ReduceType,redsort_end_rec.getUniqueID()),
			redsort_end_rec
		);
		
		log.debug("Collecting record " + redred_start_rec + 
			"("+redred_start_rec.state_type+") (ReduceType "+FSM_CRK_ReduceType+")");
		output.collect(
			new ChukwaRecordKey(FSM_CRK_ReduceType,redred_start_rec.getUniqueID()),
			redred_start_rec
		);
		
		log.debug("Collecting record " + redred_end_rec + 
			"("+redred_end_rec.state_type+") (ReduceType "+FSM_CRK_ReduceType+")");	
		output.collect(
			new ChukwaRecordKey(FSM_CRK_ReduceType,redred_end_rec.getUniqueID()),
			redred_end_rec
		);
		
		return true;
	}

} // end of mapper class

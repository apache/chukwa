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
public class DataNodeClientTraceMapper 
  extends MapReduceBase 
  implements Mapper<ChukwaRecordKey, ChukwaRecord, ChukwaRecordKey, FSMIntermedEntry> 
{
  private static Log log = LogFactory.getLog(FSMBuilder.class);
	protected static final String SEP = "/";
	protected final static String FSM_CRK_ReduceType = FSMType.NAMES[FSMType.FILESYSTEM_FSM];
	private final Pattern ipPattern =
    Pattern.compile(".*[a-zA-Z\\-_:\\/]([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)[a-zA-Z0-9\\-_:\\/].*");

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
		  if (val.getValue("op").startsWith("HDFS")) { 
	      parseClientTraceDetailed(key, val, output, reporter, fieldNamesList);
	    } // drop non-HDFS operations
		} 
		// ignore "DataNode" type log messages; unsupported
				
  } // end of map()

  protected final int DEFAULT_READ_DURATION_MS = 10;

  // works with <= 0.20 ClientTrace with no durations
  // includes hack to create start+end entries immediately
  protected void parseClientTraceDetailed
    (ChukwaRecordKey key, ChukwaRecord val,
     OutputCollector<ChukwaRecordKey, FSMIntermedEntry> output, 
		 Reporter reporter, ArrayList<String> fieldNamesList)
    throws IOException
  {
    FSMIntermedEntry start_rec, end_rec;
    String current_op = null, src_add = null, dest_add = null;
    String datanodeserver_add = null, blkid = null, cli_id = null;
    
    /* initialize state records */
    start_rec = new FSMIntermedEntry();
    end_rec = new FSMIntermedEntry();
    start_rec.fsm_type = new FSMType(FSMType.FILESYSTEM_FSM);
    start_rec.state_type = new StateType(StateType.STATE_START);
    end_rec.fsm_type = new FSMType(FSMType.FILESYSTEM_FSM);
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
    Matcher datanodeserver_regex = ipPattern.matcher(val.getValue("srvID"));
    if (datanodeserver_regex.matches()) {
      datanodeserver_add = datanodeserver_regex.group(1);
    } else {
      log.warn("Failed to match DataNode server address:"+val.getValue("srvID")+"");
      datanodeserver_add = "";
    }
    
    start_rec.host_exec = src_add;
    end_rec.host_exec = src_add;
        
    blkid = val.getValue("blockid").trim();
    if (fieldNamesList.contains("cliID")) {
      cli_id = val.getValue("cliID").trim();
      if (cli_id.startsWith("DFSClient_")) {
        cli_id = cli_id.substring(10);
      }
    } else {
      cli_id = "";
    }
    current_op = val.getValue("op");
    String [] k = key.getKey().split("/");    
    
    long actual_time_ms = Long.parseLong(val.getValue("actual_time"));
    if (fieldNamesList.contains("duration")) {
      try {
        actual_time_ms -= (Long.parseLong(val.getValue("duration").trim()) / 1000);
      } catch (NumberFormatException nef) {
        log.warn("Failed to parse duration: >>" + val.getValue("duration"));
      }
    } else {
      actual_time_ms -= DEFAULT_READ_DURATION_MS;
    }
    
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

    end_rec.job_id = cli_id; // use job id = block id
    start_rec.job_id = cli_id; 
      
    if (current_op.equals("HDFS_READ")) {
      if (src_add != null && src_add.equals(dest_add)) {
        start_rec.state_hdfs = new HDFSState(HDFSState.READ_LOCAL);
      } else {
        start_rec.state_hdfs = new HDFSState(HDFSState.READ_REMOTE);
      }
      // should these ALWAYS be dest?
      start_rec.host_other = dest_add;
      end_rec.host_other = dest_add;
    } else if (current_op.equals("HDFS_WRITE")) {
      if (src_add != null && dest_add.equals(datanodeserver_add)) {
        start_rec.state_hdfs = new HDFSState(HDFSState.WRITE_LOCAL);
        } else if (!dest_add.equals(datanodeserver_add)) {
        start_rec.state_hdfs = new HDFSState(HDFSState.WRITE_REMOTE);
      } else {
        start_rec.state_hdfs = new HDFSState(HDFSState.WRITE_REPLICATED);
      }
      start_rec.host_other = dest_add;
      end_rec.host_other = dest_add;
    } else {
      log.warn("Invalid state: " + current_op);
    }
    end_rec.state_hdfs = start_rec.state_hdfs;
    start_rec.state_name = start_rec.state_hdfs.toString();
    end_rec.state_name = end_rec.state_hdfs.toString();
    start_rec.identifier = blkid;
    end_rec.identifier = blkid;
    
    start_rec.unique_id = new StringBuilder().append(start_rec.state_name).append("@").append(start_rec.identifier).append("@").append(start_rec.job_id).toString();
    end_rec.unique_id = new StringBuilder().append(end_rec.state_name).append("@").append(end_rec.identifier).append("@").append(end_rec.job_id).toString();
      
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

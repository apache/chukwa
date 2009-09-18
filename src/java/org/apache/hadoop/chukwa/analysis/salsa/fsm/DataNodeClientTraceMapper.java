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
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Iterator;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.TreeSet;
import java.util.regex.*;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.chukwa.extraction.demux.*;
import org.apache.hadoop.chukwa.extraction.engine.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.filecache.DistributedCache;

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
	protected static String FSM_CRK_ReduceType = FSMType.NAMES[FSMType.FILESYSTEM_FSM];
	private final Pattern ipPattern =
    Pattern.compile(".*[a-zA-Z\\-_:\\/]([0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+)[a-zA-Z\\-_:\\/].*");
  private final Pattern logMsgPattern = Pattern.compile("^(.{23}) ([A-Z]+) ([a-zA-Z0-9\\.]+): (.*)");

  public void map
    (ChukwaRecordKey key, ChukwaRecord val,
     OutputCollector<ChukwaRecordKey, FSMIntermedEntry> output, 
		 Reporter reporter)
    throws IOException 
  {
		String newkey = new String("");
		String key_trimmed = key.toString().trim();
		String task_type;
		FSMIntermedEntry this_rec = new FSMIntermedEntry(); 

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
				
		return;
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
      src_add = new String("");
    }
    Matcher dest_regex = ipPattern.matcher(val.getValue("dest"));
    if (dest_regex.matches()) {
      dest_add = dest_regex.group(1);
    } else {
      log.warn("Failed to match dest IP:"+val.getValue("dest")+"");
      dest_add = new String("");
    }
    Matcher datanodeserver_regex = ipPattern.matcher(val.getValue("srvID"));
    if (datanodeserver_regex.matches()) {
      datanodeserver_add = datanodeserver_regex.group(1);
    } else {
      log.warn("Failed to match DataNode server address:"+val.getValue("srvID")+"");
      datanodeserver_add = new String("");
    }
    
    start_rec.host_exec = new String(datanodeserver_add);
    end_rec.host_exec = new String(datanodeserver_add);
        
    blkid = val.getValue("blockid").trim();
    if (fieldNamesList.contains("cliID")) {
      cli_id = val.getValue("cliID").trim();
      if (cli_id.startsWith("DFSClient_")) {
        cli_id = cli_id.substring(10);
      }
    } else {
      cli_id = new String("");
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
    start_rec.time_orig = (new Long(actual_time_ms)).toString(); // not actually used
    start_rec.timestamp = (new Long(actual_time_ms)).toString();
    start_rec.time_end = new String("");
    start_rec.time_start = new String(start_rec.timestamp);
    
    end_rec.time_orig_epoch = k[0];
    end_rec.time_orig = val.getValue("actual_time");    
    end_rec.timestamp = new String(val.getValue("actual_time"));
    end_rec.time_end = new String(val.getValue("actual_time"));
    end_rec.time_start = new String("");
    
    log.debug("Duration: " + (Long.parseLong(end_rec.time_end) - Long.parseLong(start_rec.time_start)));

    end_rec.job_id = new String(cli_id); // use job id = block id
    start_rec.job_id = new String(cli_id); 
      
    if (current_op.equals("HDFS_READ")) {
      if (src_add != null && src_add.equals(dest_add)) {
        start_rec.state_hdfs = new HDFSState(HDFSState.READ_LOCAL);
      } else {
        start_rec.state_hdfs = new HDFSState(HDFSState.READ_REMOTE);
      }
      // should these ALWAYS be dest?
      start_rec.host_other = new String(dest_add);
      end_rec.host_other = new String(dest_add);
    } else if (current_op.equals("HDFS_WRITE")) {
      if (src_add != null && dest_add.equals(datanodeserver_add)) {
        start_rec.state_hdfs = new HDFSState(HDFSState.WRITE_LOCAL);
        } else if (dest_add != null && !dest_add.equals(datanodeserver_add)) {
        start_rec.state_hdfs = new HDFSState(HDFSState.WRITE_REMOTE);
      } else {
        start_rec.state_hdfs = new HDFSState(HDFSState.WRITE_REPLICATED);        
      }
      start_rec.host_other = new String(dest_add);
      end_rec.host_other = new String(dest_add);
    } else {
      log.warn("Invalid state: " + current_op);
    }
    end_rec.state_hdfs = start_rec.state_hdfs;
    start_rec.state_name = start_rec.state_hdfs.toString();
    end_rec.state_name = end_rec.state_hdfs.toString();
    start_rec.identifier = new String(blkid);
    end_rec.identifier = new String(blkid);
    
    start_rec.unique_id = new String(start_rec.state_name + "@" + 
      start_rec.identifier + "@" + start_rec.job_id);
    end_rec.unique_id = new String(end_rec.state_name + "@" + 
      end_rec.identifier + "@" + end_rec.job_id);
      
    start_rec.add_info.put(Record.tagsField,val.getValue(Record.tagsField));
		start_rec.add_info.put("csource",val.getValue("csource"));
    end_rec.add_info.put(Record.tagsField,val.getValue(Record.tagsField));
		end_rec.add_info.put("csource",val.getValue("csource"));
		end_rec.add_info.put("STATE_STRING",new String("SUCCESS")); // by default
		
		// add counter value
		end_rec.add_info.put("BYTES",val.getValue("bytes"));
		    
    String crk_mid_string_start = new String(start_rec.getUniqueID() + "_" + start_rec.timestamp);
    String crk_mid_string_end = new String(end_rec.getUniqueID() + "_" + start_rec.timestamp);
    output.collect(new ChukwaRecordKey(FSM_CRK_ReduceType, crk_mid_string_start), start_rec);
    output.collect(new ChukwaRecordKey(FSM_CRK_ReduceType, crk_mid_string_end), end_rec);
    
    return;
  }

} // end of mapper class

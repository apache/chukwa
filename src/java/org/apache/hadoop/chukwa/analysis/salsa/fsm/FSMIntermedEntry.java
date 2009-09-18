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
import java.util.Set;

import org.apache.hadoop.io.WritableComparable;

/*
 * FSM Intermediate State Entry
 * 
 * Each state corresponds to two of these entries:
 * One corresponding to the start of the state, one corresponding to the end of the state
 *
 * Intermediate data-structure passed from Maps to Reduces
 *
 */
public class FSMIntermedEntry 
	implements Cloneable, WritableComparable 
{
	private final char DELIM = 1;
	
	/* Begin fields */
	public StateType state_type;
	public MapRedState state_mapred;
	public HDFSState state_hdfs;
	public FSMType fsm_type;
	
	public String state_name;
	public String identifier;
	public String unique_id; // state name + unique identifier 
														// (state-dependent)
														// this id should also correspond 
														// to the k2 value between
														// mappers and reducers
														
	public String timestamp;
	public String time_start;
	public String time_end;
	
	public String host_exec;
	public String host_other; // for instance, source host for shuffle, 
														// src/dest host for dfs read/write
	
	// These values filled in by splitting the original 
	// ChukwaRecordKey from Demux
	public String time_orig_epoch;
	public String time_orig;
	public String job_id; // we get this for free from the CRK
	
	TreeMap<String,String> add_info; // additional information 
																	 // e.g. locality information
																	
	/* End of fields */
	
	public FSMIntermedEntry() {
		this.state_mapred = new MapRedState(MapRedState.NONE);
		this.state_hdfs = new HDFSState(HDFSState.NONE);
		this.state_type = new StateType(StateType.STATE_NOOP);			
		this.add_info = new TreeMap<String, String>();
		this.host_other = new String("");
		this.job_id = new String("");
		this.time_orig_epoch = new String("");
		this.time_orig = new String("");
	}
	
	public String getUniqueID()
	{
		return new String(this.unique_id);
	}
	
	public String getFriendlyID()
	{
		return new String(this.identifier);
	}
	
	/**
	 * Set state_type and identifier before calling
	 */
	public void generateUniqueID()
	{
		if (this.fsm_type.val == FSMType.MAPREDUCE_FSM || 
			  this.fsm_type.val == FSMType.MAPREDUCE_FSM_INCOMPLETE) 
		{
			this.state_name = new String(this.state_mapred.toString());
		} else if (this.fsm_type.val == FSMType.MAPREDUCE_FSM || 
							 this.fsm_type.val == FSMType.MAPREDUCE_FSM_INCOMPLETE) 
		{
			this.state_name = new String(this.state_hdfs.toString());
		}
		this.unique_id = new String(this.state_name + "@" + this.identifier);
	}	
	
	public void write(DataOutput out) throws IOException {
		Set<String> mapKeys;
		
		out.writeInt(this.state_type.val);
		out.writeInt(this.state_mapred.val);
		out.writeInt(this.state_hdfs.val);
		out.writeInt(this.fsm_type.val);
		out.writeChar(DELIM);
		out.writeInt(state_name.length());
		if (state_name.length() > 0) out.writeUTF(state_name);
		out.writeInt(unique_id.length());
		if (unique_id.length() > 0) out.writeUTF(unique_id);
		out.writeInt(timestamp.length());
		if (timestamp.length() > 0) out.writeUTF(timestamp);
		out.writeInt(time_start.length());
		if (time_start.length() > 0) out.writeUTF(time_start);
		out.writeInt(time_end.length());
		if (time_end.length() > 0) out.writeUTF(time_end);
		out.writeInt(host_exec.length());
		if (host_exec.length() > 0) out.writeUTF(host_exec);
		out.writeInt(host_other.length());
		if (host_other.length() > 0) out.writeUTF(host_other);
		out.writeInt(time_orig_epoch.length());
		if (time_orig_epoch.length() > 0) out.writeUTF(time_orig_epoch);
		out.writeInt(time_orig.length());
		if (time_orig.length() > 0) out.writeUTF(time_orig);
		out.writeInt(job_id.length());
		if (job_id.length() > 0) out.writeUTF(job_id);
		out.writeInt(identifier.length());
		if (identifier.length() > 0) out.writeUTF(identifier);
					
		mapKeys = this.add_info.keySet();
		out.writeInt(mapKeys.size());
		
		Iterator<String> keyIter = mapKeys.iterator(); 
		
		for (int i = 0; i < mapKeys.size(); i++) {
			assert(keyIter.hasNext());
			String currKey = keyIter.next();
			if (currKey != null) {
				String currvalue = this.add_info.get(currKey);
				out.writeUTF(currKey);
				out.writeInt(currvalue.length());
				if (currvalue.length() > 0) {
					out.writeUTF(currvalue);
				} 
			} else {
				out.writeUTF(new String("NULL"));
				out.writeInt(0);
			}
		}
	}

	public void readFields(DataInput in) throws IOException {
		int currlen, numkeys;
		
		this.state_type = new StateType(in.readInt());
		this.state_mapred = new MapRedState(in.readInt());
		this.state_hdfs = new HDFSState(in.readInt());
		this.fsm_type = new FSMType(in.readInt());
		in.readChar();

		currlen = in.readInt();
		if (currlen > 0) this.state_name = in.readUTF();
		else this.state_name = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.unique_id = in.readUTF();
		else this.unique_id = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.timestamp = in.readUTF();
		else this.timestamp = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.time_start = in.readUTF();
		else this.time_start = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.time_end = in.readUTF();
		else this.time_end = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.host_exec = in.readUTF();
		else this.host_exec = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.host_other = in.readUTF();
		else this.host_other = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.time_orig_epoch = in.readUTF();
		else this.time_orig_epoch = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.time_orig = in.readUTF();
		else this.time_orig = new String("");

		currlen = in.readInt();
		if (currlen > 0) this.job_id = in.readUTF();
		else this.job_id = new String("");
			
		currlen = in.readInt();
		if (currlen > 0) this.identifier = in.readUTF();
		else this.identifier = new String("");			
					
		numkeys = in.readInt();

		this.add_info = new TreeMap<String, String>();
		
		if (numkeys > 0) {
			for (int i = 0; i < numkeys; i++) {
				String currkey, currval;
				currkey = in.readUTF();
				currlen = in.readInt();
				if (currlen > 0) {
					currval = in.readUTF();
					this.add_info.put(currkey, currval);
				}
			}
		}
	}
	 
	
	public boolean equals (Object o) {
		FSMIntermedEntry other = (FSMIntermedEntry) o;
		return this.unique_id.equals(other.unique_id);
	}
	
	public int compareTo (Object o) {
		FSMIntermedEntry other = (FSMIntermedEntry) o;
		return this.unique_id.compareTo(other.unique_id);
	}
	
	/*
	 * This method is to support convenient creating of new copies
	 * of states for Reduce to create sub-states ReduceShuffle, ReduceSort, and ReduceReducer
	 */
	public FSMIntermedEntry clone() throws CloneNotSupportedException {
		FSMIntermedEntry newObj = (FSMIntermedEntry) super.clone();
		Set<String> mapKeys;

		newObj.state_type = new StateType(this.state_type.val);
		newObj.state_mapred = new MapRedState(this.state_mapred.val);
		newObj.state_hdfs = new HDFSState(this.state_hdfs.val);
		newObj.fsm_type = new FSMType(this.fsm_type.val);

		/* Deep copy all strings */
		newObj.state_name = new String(this.state_name);
		newObj.unique_id = new String(this.unique_id);
		newObj.timestamp = new String(this.timestamp);
		newObj.time_start = new String(this.time_start);
		newObj.time_end = new String(this.time_end);
		
		newObj.time_orig_epoch = new String(this.time_orig_epoch);
		newObj.time_orig = new String(this.time_orig);
		newObj.job_id = new String(this.job_id);
		
		
		/* Deep copy of TreeMap */
		newObj.add_info = new TreeMap<String,String>();
		mapKeys = this.add_info.keySet();
		Iterator<String> keyIter = mapKeys.iterator();
		String currKey = null;
		
		for (int i = 0; i < mapKeys.size(); i++) {
			assert(keyIter.hasNext());
			currKey = keyIter.next();
			if (currKey != null) {
				newObj.add_info.put(currKey, this.add_info.get(currKey));
			}
		}
		
		return newObj;
	}
	
	public String toString() {
		return new String(this.state_name + "@" + this.unique_id);
	}
	
}
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

import java.util.StringTokenizer;

/**
 * Parse Utilities for Parsing ChukwaRecords for FSMBuilder Mappers
 *
 */

public class ParseUtilities {
	
	public static FSMIntermedEntry splitChukwaRecordKey
		(String origkey, FSMIntermedEntry rec, String delim) 
		throws Exception
	{
		StringTokenizer st = new StringTokenizer(origkey, delim);
		if (st.countTokens() != 3) {
			throw new Exception("Expected 3 tokens from ChukwaRecordKey but only found " + st.countTokens() + ".");
		}
		rec.time_orig_epoch = new String(st.nextToken());
		rec.job_id = new String(st.nextToken());
		rec.time_orig = new String(st.nextToken());
		return rec;
	}
	
	public static String extractHostnameFromTrackerName (String trackerName) 
	{
		int firstPos = "tracker_".length();
		int secondPos;
		String hostname = new String("");
		
		if (trackerName.startsWith("tracker_")) {
			secondPos = trackerName.indexOf(":",firstPos);
			hostname = trackerName.substring(firstPos, secondPos);
		}
		
		return hostname;
	}
	
	public static String removeRackFromHostname (String origHostname) 
	{
		int pos = origHostname.lastIndexOf("/");
		if (pos > -1) {
			return new String(origHostname.substring(pos));
		} else {
			return new String(origHostname);
		}
	}
	
}
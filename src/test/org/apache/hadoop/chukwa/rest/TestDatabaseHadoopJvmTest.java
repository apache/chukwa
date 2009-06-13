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

package org.apache.hadoop.chukwa.rest;

import junit.framework.TestCase;

import java.util.*;
import java.sql.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.database.DatabaseConfig;
import org.apache.hadoop.chukwa.database.Macro;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

public class TestDatabaseHadoopJvmTest extends DatabaseRestServerSetup {
    /* testing setup */
    public TestDatabaseHadoopJvmTest() {
	super();
	this.tableName="hadoop_jvm";
	this.insertString="insert into [hadoop_jvm] ("+
	    "timestamp, host, process_name, gc_timemillis, gc_count, log_error, log_fatal, log_info, log_warn, mem_heap_committed_m, mem_heap_used_m, mem_non_heap_committed_m, mem_non_heap_used_m, threads_blocked, threads_new, threads_runnable, threads_terminated, threads_timed_waiting, threads_waiting"+
	    ") values (\""+this.dateString+"\", \"host\", \"process\",1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16)";
	this.deleteString="delete from [hadoop_jvm] where "+
	    "timestamp=\""+this.dateString+"\" and host=\"host\" and process_name=\"process\"";
    }

    /* run the test */
    public void test() { 
	System.err.println("start test");
	// test xml
	try {
	    String string = resource.path("hadoop_jvm/timestamp/"+time.getTime()).accept("application/xml").get(String.class);
	    checkXML(string);
	    
	    string = resource.path("hadoop_jvm/timestamp/"+time.getTime()).accept("application/json").get(String.class);
	    checkJson(string);
	    
	    string = resource.path("hadoop_jvm/timestamp/"+time.getTime()).accept("text/csv").get(String.class);
	    checkCsv(string);
	} catch (Exception e) {
	    System.err.println("error:"+e.toString());
	}
    }
}

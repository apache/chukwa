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

public class TestDatabaseClientTraceTest extends DatabaseRestServerSetup {
    /* testing setup */
    public TestDatabaseClientTraceTest() {
	super();
	this.tableName="ClientTrace";
	this.insertString="insert into [ClientTrace] ("+
	    "timestamp, local_hdfs_read, intra_rack_hdfs_read, inter_rack_hdfs_read, local_hdfs_write, intra_rack_hdfs_write, inter_rack_hdfs_write, local_mapred_shuffle, intra_rack_mapred_shuffle, inter_rack_mapred_shuffle"+
	    ") values (\""+this.dateString+"\", 1,2,3,4,5,6,7,8,9)";
	this.deleteString="delete from [ClientTrace] where "+
	    "timestamp=\""+this.dateString+"\"";
    }

    /* run the test */
    public void test() { 
	// test xml
	String string = resource.path("client_trace/timestamp/"+time.getTime()).accept("application/xml").get(String.class);
	checkXML(string);

	string = resource.path("client_trace/timestamp/"+time.getTime()).accept("application/json").get(String.class);
	checkJson(string);

	string = resource.path("client_trace/timestamp/"+time.getTime()).accept("text/csv").get(String.class);
	checkCsv(string);
    }
}

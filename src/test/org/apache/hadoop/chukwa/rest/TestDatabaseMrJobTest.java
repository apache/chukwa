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

public class TestDatabaseMrJobTest extends DatabaseRestServerSetup {
    /* testing setup */
    public TestDatabaseMrJobTest() {
	super();
	this.tableName="mr_job";
	this.insertString="insert into [mr_job] ("+
	    "job_id,submit_time,launch_time,finish_time"+
	    ") values (\""+time.getTime()+"\",\""+this.dateString+"\",\""+this.dateString+"\",\""+this.dateString+"\")";
	this.deleteString="delete from [mr_job] where "+
	    "job_id=\""+time.getTime()+"\" ";
    }

    /* run the test */
    public void test() { 
	// test xml
	String string = resource.path("mr_job/job_id/"+time.getTime()).accept("application/xml").get(String.class);
	checkXML(string);

	string = resource.path("mr_job/job_id/"+time.getTime()).accept("application/json").get(String.class);
	checkJson(string);

	string = resource.path("mr_job/job_id/"+time.getTime()).accept("text/csv").get(String.class);
	checkCsv(string);
    }
}

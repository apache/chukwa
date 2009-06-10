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

package org.apache.hadoop.chukwa.rest.services;

import java.util.*;
import java.sql.*;
import javax.naming.InitialContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.database.DatabaseConfig;
import org.apache.hadoop.chukwa.database.Macro;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

import org.apache.hadoop.chukwa.rest.objects.ClientTrace;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class HadoopJvm.
 * @see org.apahe.hadoop.chukwa.rest.objects.ClientTrace
 * @author Hibernate Tools
 */
public class ClientTraceHome extends RestHome {
    private static String table="[ClientTrace]";
    private static final Log log = LogFactory
	.getLog(ClientTraceHome.class);

    private static ClientTrace createClientTrace(ResultSet rs) {
	ClientTrace obj=null;
	try {
	    obj = new ClientTrace(
				  rs.getTimestamp("Timestamp"),
				  rs.getDouble("local_hdfs_read"),
				  rs.getDouble("intra_rack_hdfs_read"),
				  rs.getDouble("inter_rack_hdfs_read"),
				  rs.getDouble("local_hdfs_write"),
				  rs.getDouble("intra_rack_hdfs_write"),
				  rs.getDouble("inter_rack_hdfs_write"),
				  rs.getDouble("local_mapred_shuffle"),
				  rs.getDouble("intra_rack_mapred_shuffle"),
				  rs.getDouble("inter_rack_mapred_shuffle")
				  );
	} catch (Exception e) {	    
	}
	return obj;
    }
    
    public static ClientTrace find(String timestamp) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		String query = getSingleQuery(ClientTraceHome.table,"timestamp",timestamp);
		log.error(query);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    log.error("find it.");
		    ClientTrace obj = createClientTrace(rs);
		    return obj;
		}
	    } catch (Exception e) {
		log.error("exception:"+e.toString());
	    }
	} else {
	    // check start time and end time
	}
	return null;
    }

    public static Collection<ClientTrace> findBetween(String starttime, String endtime) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	Collection<ClientTrace> collection = new Vector<ClientTrace>();

	try {
	    String query = getTimeBetweenQuery(ClientTraceHome.table,starttime,endtime);	    
	    ResultSet rs = dbw.query(query);
	    while (rs.next()) {
		ClientTrace obj = createClientTrace(rs);
		collection.add(obj);
	    }
	} catch (Exception e) {
	    log.error("exception:"+e.toString());
	}
	return collection;
    }
}

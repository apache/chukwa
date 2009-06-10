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

import org.apache.hadoop.chukwa.rest.objects.HadoopJvm;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class HadoopJvm.
 * @see org.apahe.hadoop.chukwa.rest.objects.HadoopJvm
 * @author Hibernate Tools
 */
public class HadoopJvmHome extends RestHome {
    private static String table="[hadoop_jvm]";
    private static final Log log = LogFactory
	.getLog(HadoopJvmHome.class);

    /*
     * convert from a result set record to an object
     */
    private static HadoopJvm createHadoopJvm(ResultSet rs) {
	HadoopJvm obj = null;
	try {
	    obj = new HadoopJvm(
				      rs.getTimestamp("timestamp"),
				      rs.getString("host"),
				      rs.getString("process_name"),

				      rs.getDouble("gc_timemillis"),
				      rs.getDouble("gc_count"),
				      rs.getDouble("log_error"),
				      rs.getDouble("log_fatal"),
				      rs.getDouble("log_info"),
				      rs.getDouble("log_warn"),
				      rs.getDouble("mem_heap_committed_m"),
				      rs.getDouble("mem_heap_used_m"),
				      rs.getDouble("mem_non_heap_committed_m"),
				      rs.getDouble("mem_non_heap_used_m"),
				      rs.getDouble("threads_blocked"),
				      rs.getDouble("threads_new"),
				      rs.getDouble("threads_runnable"),
				      rs.getDouble("threads_terminated"),
				      rs.getDouble("threads_timed_waiting"),
				      rs.getDouble("threads_waiting")
				      );
	} catch (Exception e) {
	}
	return obj;
    }
    
    /*
     * find by timestamp
     */
    public static HadoopJvm find(String timestamp) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		String query = getSingleQuery(HadoopJvmHome.table,"timestamp",timestamp);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    HadoopJvm obj = createHadoopJvm(rs);
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

    /*
     * find by key 
     */
    public static HadoopJvm find(String timestamp, String host, String process_name) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		Map<String, String> criteriaMap = new HashMap<String,String>();
		criteriaMap.put("timestamp",convertLongToDateString(Long.parseLong(timestamp)));
		criteriaMap.put("host",host);
		criteriaMap.put("process_name",process_name);

		String query = getCriteriaQuery(HadoopJvmHome.table,criteriaMap);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    HadoopJvm obj = createHadoopJvm(rs);
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

    /*
     * find within the start time and end time
     */
    public static Collection<HadoopJvm> findBetween(String starttime, String endtime) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	Collection<HadoopJvm> collection = new Vector<HadoopJvm>();

	try {
	    String query = getTimeBetweenQuery(HadoopJvmHome.table,starttime,endtime);	    
	    ResultSet rs = dbw.query(query);
	    while (rs.next()) {
		HadoopJvm obj = createHadoopJvm(rs);
		collection.add(obj);
	    }
	} catch (Exception e) {
	    log.error("exception:"+e.toString());
	}
	return collection;
    }
}

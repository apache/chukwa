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

import org.apache.hadoop.chukwa.rest.objects.HadoopRpc;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class HadoopRpc.
 * @see org.apahe.hadoop.chukwa.rest.objects.HadoopRpc
 * @author Hibernate Tools
 */
public class HadoopRpcHome extends RestHome {
    private static String table="[hadoop_rpc]";
    private static final Log log = LogFactory
	.getLog(HadoopRpcHome.class);

    /*
     * convert from a result set record to an object
     */
    private static HadoopRpc createHadoopRpc(ResultSet rs) {
	HadoopRpc obj = null;
	try {
	    obj = new HadoopRpc(
				      rs.getTimestamp("timestamp"),
				      rs.getString("host"),
				      rs.getDouble("rpc_processing_time_avg_time"),
				      rs.getDouble("rpc_processing_time_num_ops"),
				      rs.getDouble("rpc_queue_time_avg_time"),
				      rs.getDouble("rpc_queue_time_num_ops"),
				      rs.getDouble("get_build_version_avg_time"),
				      rs.getDouble("get_build_version_num_ops"),
				      rs.getDouble("get_job_counters_avg_time"),
				      rs.getDouble("get_job_counters_num_ops"),
				      rs.getDouble("get_job_profile_avg_time"),
				      rs.getDouble("get_job_profile_num_ops"),
				      rs.getDouble("get_job_status_avg_time"),
				      rs.getDouble("get_job_status_num_ops"),
				      rs.getDouble("get_new_job_id_avg_time"),
				      rs.getDouble("get_new_job_id_num_ops"),
				      rs.getDouble("get_protocol_version_avg_time"),
				      rs.getDouble("get_protocol_version_num_ops"),
				      rs.getDouble("get_system_dir_avg_time"),
				      rs.getDouble("get_system_dir_num_ops"),
				      rs.getDouble("get_task_completion_events_avg_time"),
				      rs.getDouble("get_task_completion_events_num_ops"),
				      rs.getDouble("get_task_diagnostics_avg_time"),
				      rs.getDouble("get_task_diagnostics_num_ops"),
				      rs.getDouble("heartbeat_avg_time"),
				      rs.getDouble("heartbeat_num_ops"),
				      rs.getDouble("killJob_avg_time"),
				      rs.getDouble("killJob_num_ops"),
				      rs.getDouble("submit_job_avg_time"),
				      rs.getDouble("submit_job_num_ops")
				      );
	} catch (Exception e) {
	}
	return obj;
    }
    
    /*
     * find by timestamp
     */
    public static HadoopRpc find(String timestamp) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		String query = getSingleQuery(HadoopRpcHome.table,"timestamp",timestamp);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    HadoopRpc obj = createHadoopRpc(rs);
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
    public static HadoopRpc find(String timestamp, String host) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		Map<String, String> criteriaMap = new HashMap<String,String>();
		criteriaMap.put("timestamp",convertLongToDateString(Long.parseLong(timestamp)));
		criteriaMap.put("host",host);

		String query = getCriteriaQuery(HadoopRpcHome.table,criteriaMap);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    HadoopRpc obj = createHadoopRpc(rs);
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
    public static Collection<HadoopRpc> findBetween(String starttime, String endtime) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	Collection<HadoopRpc> collection = new Vector<HadoopRpc>();

	try {
	    String query = getTimeBetweenQuery(HadoopRpcHome.table,starttime,endtime);	    
	    ResultSet rs = dbw.query(query);
	    while (rs.next()) {
		HadoopRpc obj = createHadoopRpc(rs);
		collection.add(obj);
	    }
	} catch (Exception e) {
	    log.error("exception:"+e.toString());
	}
	return collection;
    }
}

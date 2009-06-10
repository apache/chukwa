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

import org.apache.hadoop.chukwa.rest.objects.ClusterSystemMetrics;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class HadoopJvm.
 * @see org.apahe.hadoop.chukwa.rest.objects.ClusterSystemMetrics
 * @author Hibernate Tools
 */
public class ClusterSystemMetricsHome extends RestHome {
    private static String table="[cluster_system_metrics]";
    private static final Log log = LogFactory
	.getLog(ClusterSystemMetricsHome.class);

    private static ClusterSystemMetrics createClusterSystemMetrics(ResultSet rs) {
	ClusterSystemMetrics obj=null;
	try {
	    obj= new ClusterSystemMetrics(
					  rs.getTimestamp("Timestamp"),
					  rs.getInt("host"),
					  rs.getDouble("load_15"),
					  rs.getDouble("load_5"),
					  rs.getDouble("load_1"),
					  rs.getDouble("task_total"),
					  rs.getDouble("task_running"),
					  rs.getDouble("task_sleep"),
					  rs.getDouble("task_stopped"),
					  rs.getDouble("task_zombie"),
					  rs.getDouble("mem_total"),
					  rs.getDouble("mem_buffers"),
					  rs.getDouble("mem_cached"),
					  rs.getDouble("mem_used"),
					  rs.getDouble("mem_free"),
					  rs.getDouble("eth0_rxerrs"),
					  rs.getDouble("eth0_rxbyts"),
					  rs.getDouble("eth0_rxpcks"),
					  rs.getDouble("eth0_rxdrops"),
					  rs.getDouble("eth0_txerrs"),
					  rs.getDouble("eth0_txbyts"),
					  rs.getDouble("eth0_txpcks"),
					  rs.getDouble("eth0_txdrops"),
					  rs.getDouble("eth1_rxerrs"),
					  rs.getDouble("eth1_rxbyts"),
					  rs.getDouble("eth1_rxpcks"),
					  rs.getDouble("eth1_rxdrops"),
					  rs.getDouble("eth1_txerrs"),
					  rs.getDouble("eth1_txbyts"),
					  rs.getDouble("eth1_txpcks"),
					  rs.getDouble("eth1_txdrops"),
					  rs.getDouble("sda_rkbs"),
					  rs.getDouble("sda_wkbs"),
					  rs.getDouble("sdb_rkbs"),
					  rs.getDouble("sdb_wkbs"),
					  rs.getDouble("sdc_rkbs"),
					  rs.getDouble("sdc_wkbs"),
					  rs.getDouble("sdd_rkbs"),
					  rs.getDouble("sdd_wkbs"),
					  rs.getFloat("cpu_idle_pcnt"),
					  rs.getFloat("cpu_nice_pcnt"),
					  rs.getFloat("cpu_system_pcnt"),
					  rs.getFloat("cpu_user_pcnt"),
					  rs.getFloat("cpu_hirq_pcnt"),
					  rs.getFloat("cpu_sirq_pcnt"),
					  rs.getFloat("iowait_pcnt"),
					  rs.getFloat("mem_buffers_pcnt"),
					  rs.getFloat("mem_used_pcnt"),
					  rs.getFloat("eth0_busy_pcnt"),
					  rs.getFloat("eth1_busy_pcnt"),
					  rs.getFloat("sda_busy_pcnt"),
					  rs.getFloat("sdb_busy_pcnt"),
					  rs.getFloat("sdc_busy_pcnt"),
					  rs.getFloat("sdd_busy_pcnt"),
					  rs.getFloat("swap_used_pcnt")
					  );
	} catch (Exception e) {
	}
	return obj;
    }
    
    public static ClusterSystemMetrics find(String timestamp) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		String query = getSingleQuery(ClusterSystemMetricsHome.table,"timestamp",timestamp);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    ClusterSystemMetrics obj = createClusterSystemMetrics(rs);
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

    public static Collection<ClusterSystemMetrics> findBetween(String starttime, String endtime) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	Collection<ClusterSystemMetrics> collection = new Vector<ClusterSystemMetrics>();

	try {
	    String query = getTimeBetweenQuery(ClusterSystemMetricsHome.table,starttime,endtime);	    
	    ResultSet rs = dbw.query(query);
	    while (rs.next()) {
		ClusterSystemMetrics obj = createClusterSystemMetrics(rs);
		collection.add(obj);
	    }
	} catch (Exception e) {
	    log.error("exception:"+e.toString());
	}
	return collection;
    }
}

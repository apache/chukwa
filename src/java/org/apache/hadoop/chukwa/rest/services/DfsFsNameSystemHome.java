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

import org.apache.hadoop.chukwa.rest.objects.DfsFsNameSystem;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class DfsFsNameSystem.
 * @see org.apahe.hadoop.chukwa.rest.objects.DfsFsNameSystem
 * @author Hibernate Tools
 */
public class DfsFsNameSystemHome extends RestHome {
    private static String table="[dfs_fsnamesystem]";
    private static final Log log = LogFactory
	.getLog(DfsFsNameSystemHome.class);

    /*
     * convert from a result set record to an object
     */
    private static DfsFsNameSystem createDfsFsNameSystem(ResultSet rs) {
	DfsFsNameSystem obj = null;
	try {
	    obj = new DfsFsNameSystem(
						  rs.getTimestamp("timestamp"),
						  rs.getString("host"),
						  rs.getDouble("blocks_total"),
						  rs.getDouble("capacity_remaining_gb"),
						  rs.getDouble("capacity_total_gb"),
						  rs.getDouble("capacity_used_gb"),
						  rs.getDouble("files_total"),
						  rs.getDouble("pending_replication_blocks"),
						  rs.getDouble("scheduled_replication_blocks"),
						  rs.getDouble("total_load"),
						  rs.getDouble("under_replicated_blocks")
						  );
	} catch (Exception e) {
	}
	return obj;
    }
    
    /*
     * find by timestamp
     */
    public static DfsFsNameSystem find(String timestamp) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		String query = getSingleQuery(DfsFsNameSystemHome.table,"timestamp",timestamp);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    DfsFsNameSystem obj = createDfsFsNameSystem(rs);
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
    public static DfsFsNameSystem find(String timestamp, String host) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		Map<String, String> criteriaMap = new HashMap<String,String>();
		criteriaMap.put("timestamp",convertLongToDateString(Long.parseLong(timestamp)));
		criteriaMap.put("host",host);

		String query = getCriteriaQuery(DfsFsNameSystemHome.table,criteriaMap);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    DfsFsNameSystem obj = createDfsFsNameSystem(rs);
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
    public static Collection<DfsFsNameSystem> findBetween(String starttime, String endtime) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	Collection<DfsFsNameSystem> collection = new Vector<DfsFsNameSystem>();

	try {
	    String query = getTimeBetweenQuery(DfsFsNameSystemHome.table,starttime,endtime);	    
	    ResultSet rs = dbw.query(query);
	    while (rs.next()) {
		DfsFsNameSystem obj = createDfsFsNameSystem(rs);
		collection.add(obj);
	    }
	} catch (Exception e) {
	    log.error("exception:"+e.toString());
	}
	return collection;
    }
}

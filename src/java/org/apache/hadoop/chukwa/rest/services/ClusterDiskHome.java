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

import org.apache.hadoop.chukwa.rest.objects.ClusterDisk;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class ClusterDisk.
 * @see org.apahe.hadoop.chukwa.rest.objects.ClusterDisk
 * @author Hibernate Tools
 */
public class ClusterDiskHome extends RestHome {

    private static String table="[cluster_disk]";
    private static final Log log = LogFactory
	.getLog(ClusterDiskHome.class);

    /*
     * convert from a result set record to an object
     */
    private static ClusterDisk createClusterDisk(ResultSet rs) {
	ClusterDisk obj = null;
	try {
	    obj = new ClusterDisk(
				  rs.getTimestamp("timestamp"),
				  rs.getString("mount"),
				  rs.getDouble("used"),
				  rs.getDouble("available"),
				  rs.getDouble("used_percent")
				  );
	} catch (Exception e) {
	}
	return obj;
    }
    
    /*
     * find by timestamp
     */
    public static ClusterDisk find(String timestamp) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		String query = getSingleQuery(ClusterDiskHome.table,"timestamp",timestamp);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    ClusterDisk obj = createClusterDisk(rs);
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
    public static ClusterDisk find(String timestamp, String mount) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		Map<String, String> criteriaMap = new HashMap<String,String>();
		criteriaMap.put("timestamp",convertLongToDateString(Long.parseLong(timestamp)));
		criteriaMap.put("mount",mount);

		String query = getCriteriaQuery(ClusterDiskHome.table,criteriaMap);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    ClusterDisk obj = createClusterDisk(rs);
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
    public static Collection<ClusterDisk> findBetween(String starttime, String endtime) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	Collection<ClusterDisk> collection = new Vector<ClusterDisk>();

	try {
	    String query = getTimeBetweenQuery(ClusterDiskHome.table,starttime,endtime);	    
	    ResultSet rs = dbw.query(query);
	    while (rs.next()) {
		ClusterDisk obj = createClusterDisk(rs);
		collection.add(obj);
	    }
	} catch (Exception e) {
	    log.error("exception:"+e.toString());
	}
	return collection;
    }
}

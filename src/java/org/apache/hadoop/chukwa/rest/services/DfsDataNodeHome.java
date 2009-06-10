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

import org.apache.hadoop.chukwa.rest.objects.DfsDataNode;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class DfsDataNode.
 * @see org.apahe.hadoop.chukwa.rest.objects.DfsDataNode
 * @author Hibernate Tools
 */
public class DfsDataNodeHome extends RestHome {
    private static String table="[dfs_datanode]";
    private static final Log log = LogFactory
	.getLog(DfsDataNodeHome.class);

    /*
     * convert from a result set record to an object
     */
    private static DfsDataNode createDfsDataNode(ResultSet rs) {
	DfsDataNode obj = null;
	try {
	    obj = new DfsDataNode(
					  rs.getTimestamp("timestamp"),
					  rs.getString("host"),
					  rs.getDouble("block_reports_avg_time"),
					  rs.getDouble("block_reports_num_ops"),
					  rs.getDouble("block_verification_failures"),
					  rs.getDouble("blocks_read"),
					  rs.getDouble("blocks_removed"),
					  rs.getDouble("blocks_replicated"),
					  rs.getDouble("blocks_verified"),
					  rs.getDouble("blocks_written"),
					  rs.getDouble("bytes_read"),
					  rs.getDouble("bytes_written"),
					  rs.getDouble("copy_block_op_avg_time"),
					  rs.getDouble("copy_block_op_num_ops"),
					  rs.getDouble("heart_beats_avg_time"),
					  rs.getDouble("heart_beats_num_ops"),
					  rs.getDouble("read_block_op_avg_time"),
					  rs.getDouble("read_block_op_num_ops"),
					  rs.getDouble("read_metadata_op_avg_time"),
					  rs.getDouble("read_metadata_op_num_ops"),
					  rs.getDouble("reads_from_local_client"),
					  rs.getDouble("reads_from_remote_client"),
					  rs.getDouble("replace_block_op_avg_time"),
					  rs.getDouble("replace_block_op_num_ops"),
					  rs.getDouble("session_id"),
					  rs.getDouble("write_block_op_avg_time"),
					  rs.getDouble("write_block_op_num_ops"),
					  rs.getDouble("writes_from_local_client"),
					  rs.getDouble("writes_from_remote_client")
					  );
	} catch (Exception e) {
	}
	return obj;
    }
    
    /*
     * find by timestamp
     */
    public static DfsDataNode find(String timestamp) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		String query = getSingleQuery(DfsDataNodeHome.table,"timestamp",timestamp);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    DfsDataNode obj = createDfsDataNode(rs);
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
    public static DfsDataNode find(String timestamp, String host) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (timestamp != null) {
	    // get simple value
            try {
		Map<String, String> criteriaMap = new HashMap<String,String>();
		criteriaMap.put("timestamp",convertLongToDateString(Long.parseLong(timestamp)));
		criteriaMap.put("host",host);

		String query = getCriteriaQuery(DfsDataNodeHome.table,criteriaMap);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    DfsDataNode obj = createDfsDataNode(rs);
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
    public static Collection<DfsDataNode> findBetween(String starttime, String endtime) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	Collection<DfsDataNode> collection = new Vector<DfsDataNode>();

	try {
	    String query = getTimeBetweenQuery(DfsDataNodeHome.table,starttime,endtime);	    
	    ResultSet rs = dbw.query(query);
	    while (rs.next()) {
		DfsDataNode obj = createDfsDataNode(rs);
		collection.add(obj);
	    }
	} catch (Exception e) {
	    log.error("exception:"+e.toString());
	}
	return collection;
    }
}

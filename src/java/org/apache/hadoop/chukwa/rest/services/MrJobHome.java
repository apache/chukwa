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

import org.apache.hadoop.chukwa.rest.objects.MrJob;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class HadoopJvm.
 * @see org.apahe.hadoop.chukwa.rest.objects.MrJob
 * @author Hibernate Tools
 */
public class MrJobHome extends RestHome {
    private static String table="[mr_job]";
    private static final Log log = LogFactory
	.getLog(MrJobHome.class);

    private static MrJob createMrJob(ResultSet rs) {
	MrJob obj = null;
	try {
	    obj = new MrJob(
			      rs.getString("job_id"),
			      rs.getString("user"),
			      rs.getString("queue"),
			      rs.getString("status"),
			      rs.getTimestamp("submit_time"),
			      rs.getTimestamp("launch_time"),
			      rs.getTimestamp("finish_time"),
			      rs.getLong("hdfs_bytes_read"),
			      rs.getLong("hdfs_bytes_written"),
			      rs.getLong("local_bytes_read"),
			      rs.getLong("local_bytes_written"),
			      rs.getLong("launched_map_tasks"),
			      rs.getLong("launched_reduce_tasks"),
			      rs.getLong("data_local_map_tasks"),
			      rs.getLong("data_local_reduce_tasks"),
			      rs.getLong("map_input_bytes"),
			      rs.getLong("map_output_bytes"),
			      rs.getLong("map_input_records"),
			      rs.getLong("map_output_records"),
			      rs.getLong("combine_input_records"),
			      rs.getLong("combine_output_records"),
			      rs.getLong("spilled_records"),
			      rs.getLong("reduce_input_groups"),
			      rs.getLong("reduce_output_groups"),
			      rs.getLong("reduce_input_records"),
			      rs.getLong("reduce_output_records"),
			      rs.getString("jobconf"),
			      rs.getLong("finished_maps"),
			      rs.getLong("finished_reduces"),
			      rs.getLong("failed_maps"),
			      rs.getLong("failed_reduces"),
			      rs.getLong("total_maps"),
			      rs.getLong("total_reduces"),
			      rs.getLong("reduce_shuffle_bytes")
			      );
	} catch (Exception e) {
	    System.err.println("error:"+e.toString());
	}
	return obj;
    }
    
    public static MrJob find(String job_id) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (job_id != null) {
	    // get simple value
            try {
		String query = getSingleQuery(MrJobHome.table,"job_id",job_id);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    MrJob obj = createMrJob(rs);
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
}

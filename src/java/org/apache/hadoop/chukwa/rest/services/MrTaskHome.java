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

import org.apache.hadoop.chukwa.rest.objects.MrTask;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class HadoopJvm.
 * @see org.apahe.hadoop.chukwa.rest.objects.MrTask
 * @author Hibernate Tools
 */
public class MrTaskHome extends RestHome {
    private static String table="[mr_task]";
    private static final Log log = LogFactory
	.getLog(MrTaskHome.class);

    private static MrTask createMrTask(ResultSet rs) {
	MrTask obj = null;
	try {
	    obj = new MrTask(

				rs.getString("task_id"),
				rs.getString("job_id"),
				rs.getTimestamp("start_time"),
				rs.getTimestamp("finish_time"),
				rs.getString("status"),
				rs.getByte("attempts"),
				rs.getLong("hdfs_bytes_read"),
				rs.getLong("hdfs_bytes_written"),
				rs.getLong("local_bytes_read"),
				rs.getLong("local_bytes_written"),
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
				rs.getLong("reduce_input_bytes"),
				rs.getLong("reduce_output_bytes"),
				rs.getString("type"),
				rs.getLong("reduce_shuffle_bytes"),
				rs.getString("hostname"),
				rs.getTimestamp("shuffle_finished"),
				rs.getTimestamp("sort_finished"),
				rs.getLong("spilts")
			      );
	} catch (Exception e) {
	}
	return obj;
    }
    
    public static MrTask find(String task_id) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);

	if (task_id != null) {
	    // get simple value
            try {
		String query = getSingleQuery(MrTaskHome.table,"task_id",task_id);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    MrTask obj = createMrTask(rs);
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

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

import org.apache.hadoop.chukwa.rest.objects.MrJobConf;
import org.apache.hadoop.chukwa.rest.services.RestHome;

/**
 * Home object for domain model class HadoopJvm.
 * @see org.apahe.hadoop.chukwa.rest.objects.MrJobConf
 * @author Hibernate Tools
 */
public class MrJobConfHome extends RestHome {
    private static String table="[mr_job_conf]";
    private static final Log log = LogFactory
	.getLog(MrJobConfHome.class);

    private static MrJobConf createMrJobConf(ResultSet rs) {
	MrJobConf obj = null;
	try {
	    obj = new MrJobConf(
				      rs.getString("job_id"),
				      rs.getTimestamp("ts"),
				      rs.getString("mr_output_key_cls"),
				      rs.getString("mr_runner_cls"),
				      rs.getString("mr_output_value_cls"),
				      rs.getString("mr_input_fmt_cls"),
				      rs.getString("mr_output_fmt_cls"),
				      rs.getString("mr_reducer_cls"),
				      rs.getString("mr_mapper_cls")
				      );
	} catch (Exception e) {
	}
	return obj;
    }
    
    public static MrJobConf find(String job_id) {
	String cluster = getCluster();
	DatabaseWriter dbw = new DatabaseWriter(cluster);
	
	if (job_id != null) {
	    // get simple value
            try {
		String query = getSingleQuery(MrJobConfHome.table,"job_id",job_id);
	    	ResultSet rs = dbw.query(query);
	    	if (rs.next()) {
		    MrJobConf obj = createMrJobConf(rs);
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

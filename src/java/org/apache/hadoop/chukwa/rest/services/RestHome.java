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
import java.text.SimpleDateFormat;
import org.apache.hadoop.chukwa.hicc.ClusterConfig;
import org.apache.commons.logging.*;
import org.apache.hadoop.chukwa.database.DatabaseConfig;
import org.apache.hadoop.chukwa.database.Macro;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.DatabaseWriter;

public class RestHome  {
    private static final String DATE_FORMAT_NOW = "yyyy-MM-dd HH:mm:ss";
    protected static Log log = LogFactory.getLog(RestHome.class);

    public static String getCluster() {
	String cluster=System.getProperty("CLUSTER", "");
	if (cluster.compareTo("")==0)  {
	    ClusterConfig cc=new ClusterConfig();
	    Iterator<String> keys = cc.getClusters();
	    if (keys.hasNext()) {
		cluster=keys.next();
	    }
	    System.setProperty("CLUSTER",cluster);
	}

        if (cluster == null) {
	    cluster = "demo";
	}

	return cluster;
    }

    public static String convertLongToDateString(long date) {
	date=date;
	SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
	log.error("date:"+sdf.format(date));
	return sdf.format(date);	
    }

    public static long convertDateLongStringToLong(String d) {
	Long l=Long.parseLong(d);
	return l;
    }

    public static String getSingleQuery(String table, String field, String timestamp) {
	Calendar now = Calendar.getInstance();

	// default time
	long startTime = now.getTimeInMillis();
	long endTime = now.getTimeInMillis();

	if (field.compareTo("timestamp")==0) {
	    startTime = Long.parseLong(timestamp);
	    endTime = Long.parseLong(timestamp);
	}

	Macro mp = new Macro(startTime, endTime, table, null);
	String query = mp.toString();
	StringBuilder queryBuilder = new StringBuilder();
	queryBuilder.append("select * from ");
	queryBuilder.append(query);
	if (field.compareTo("timestamp")==0) {
	    queryBuilder.append(" where "+field+" = \""+convertLongToDateString(Long.parseLong(timestamp))+"\"");
	} else {
	    queryBuilder.append(" where "+field+" = \""+timestamp+"\"");
	}
	query = queryBuilder.toString();
	log.error("query:"+query);
	return query.toString();
    }

    public static String getTimeBetweenQuery(String table, String starttime, String endtime) {
	long startTime = convertDateLongStringToLong(starttime)*1000;
	long endTime = convertDateLongStringToLong(endtime)*1000;

	StringBuilder queryBuilder = new StringBuilder();
	queryBuilder.append("select * from ");
	queryBuilder.append(table);
	queryBuilder.append(" where timestamp between \"[start]\" and \"[end]\"");
	String query = queryBuilder.toString();
	Macro mp = new Macro(startTime, endTime, query, null);
	query = mp.toString();
	log.error("query:"+query);
	return query;
    }

    public static String getCriteriaQuery(String table, Map<String, String> criteria) {
	Calendar now = Calendar.getInstance();

	// default time
	long startTime = now.getTimeInMillis()-(7*24*60*60*1000);
	long endTime = now.getTimeInMillis();

	Macro mp = new Macro(startTime, endTime, table, null);
	String query = mp.toString();
	StringBuilder queryBuilder = new StringBuilder();
	queryBuilder.append("select * from ");
	queryBuilder.append(query);
	queryBuilder.append(" where ");
	int count=0;
	for (Map.Entry<String, String> e : criteria.entrySet()) {
	    if (count != 0) {
		queryBuilder.append(" and ");
	    }
	    queryBuilder.append(e.getKey()+"=\""+e.getValue()+"\"");
	    count++;
	}
	query = queryBuilder.toString();
	log.error("query:"+query);
	return query;
    }

}

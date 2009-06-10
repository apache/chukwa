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

package org.apache.hadoop.chukwa.rest.actions;

import java.util.*;
import javax.ws.rs.*;

import org.apache.hadoop.chukwa.rest.objects.ClusterDisk;
import org.apache.hadoop.chukwa.rest.services.ClusterDiskHome;

@Path ("/cluster_disk")
@Produces("application/xml")
public class ClusterDiskResource extends RestController {

    // get one object
    @GET
    @Path("timestamp/{timestamp}")
    @Produces({"application/xml","text/xml"})
    public String getByTimestampXML( @PathParam ("timestamp") String timestamp) {
	ClusterDisk model = ClusterDiskHome.find(timestamp);
	return convertToXml(model);
    }
    
    @GET
    @Path("timestamp/{timestamp}")
    @Produces("application/json")
    public String getByTimestampJason( @PathParam ("timestamp") String timestamp) {
	ClusterDisk model = ClusterDiskHome.find(timestamp);
	return convertToJson(model);
    }
    
    @GET
    @Path("timestamp/{timestamp}")
    @Produces({"text/plain","text/csv"})
    public String getByTimestampCsv( @PathParam ("timestamp") String timestamp) {
	ClusterDisk model = ClusterDiskHome.find(timestamp);
	return convertToCsv(model);
    }

    // get one object timestamp + mount
    @GET
    @Path("timestamp/{timestamp}/mount/{mount}")
    @Produces({"application/xml","text/xml"})
	public String getByTimestampMountXML( @PathParam ("timestamp") String timestamp,
					 @PathParam ("mount") String mount ) {
	ClusterDisk model = ClusterDiskHome.find(timestamp, mount);
	return convertToXml(model);
    }
    
    @GET
    @Path("timestamp/{timestamp}/mount/{mount}")
    @Produces("application/json")
	public String getByTimestampMountJason( @PathParam ("timestamp") String timestamp,
					   @PathParam ("mount") String mount ) {
	ClusterDisk model = ClusterDiskHome.find(timestamp, mount);
	return convertToJson(model);
    }
    
    @GET
    @Path("timestamp/{timestamp}/mount/{mount}")
    @Produces({"text/plain","text/csv"})
	public String getByTimestampMountCsv( @PathParam ("timestamp") String timestamp,
					 @PathParam ("mount") String mount ) {
	ClusterDisk model = ClusterDiskHome.find(timestamp, mount);
	return convertToCsv(model);
    }

    // search range 
    @GET
    @Path("starttime/{starttime}/endtime/{endtime}")
    @Produces({"application/xml", "text/xml"})
    public String getByKeysXml(@PathParam("starttime") String starttime,
			    @PathParam("endtime") String endtime) {
	Collection<ClusterDisk> list = ClusterDiskHome.findBetween(starttime,endtime);
	return convertToXml(list);
    }

    @GET
    @Path("starttime/{starttime}/endtime/{endtime}")
    @Produces("application/json")
    public String getByKeysJson(@PathParam("starttime") String starttime,
			    @PathParam("endtime") String endtime) {
	Collection<ClusterDisk> list = ClusterDiskHome.findBetween(starttime,endtime);
	return convertToJson(list);
    }
    @GET
    @Path("starttime/{starttime}/endtime/{endtime}")
    @Produces({"text/plain", "text/csv"})
    public String getByKeysCsv(@PathParam("starttime") String starttime,
			    @PathParam("endtime") String endtime) {
	Collection<ClusterDisk> list = ClusterDiskHome.findBetween(starttime,endtime);
	return convertToCsv(list);
    }
}

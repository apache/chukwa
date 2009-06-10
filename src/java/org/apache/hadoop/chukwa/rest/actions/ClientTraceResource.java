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

import org.apache.hadoop.chukwa.rest.objects.ClientTrace;
import org.apache.hadoop.chukwa.rest.services.ClientTraceHome;

@Path ("/client_trace")
@Produces("application/xml")
public class ClientTraceResource extends RestController {

    // get one object
    @GET
    @Path("timestamp/{timestamp}")
    @Produces({"application/xml","text/xml"})
    public String getByTimestampXML( @PathParam ("timestamp") String timestamp) {
	ClientTrace model = ClientTraceHome.find(timestamp);
	return convertToXml(model);
    }
    
    @GET
    @Path("timestamp/{timestamp}")
    @Produces("application/json")
    public String getByTimestampJason( @PathParam ("timestamp") String timestamp) {
	ClientTrace model = ClientTraceHome.find(timestamp);
	return convertToJson(model);
    }
    
    @GET
    @Path("timestamp/{timestamp}")
    @Produces({"text/plain","text/csv"})
    public String getByTimestampCsv( @PathParam ("timestamp") String timestamp) {
	ClientTrace model = ClientTraceHome.find(timestamp);
	return convertToCsv(model);
    }

    // search range 
    @GET
    @Path("starttime/{starttime}/endtime/{endtime}")
    @Produces({"application/xml", "text/xml"})
    public String getByKeysXml(@PathParam("starttime") String starttime,
			    @PathParam("endtime") String endtime) {
	Collection<ClientTrace> list = ClientTraceHome.findBetween(starttime,endtime);
	return convertToXml(list);
    }

    @GET
    @Path("starttime/{starttime}/endtime/{endtime}")
    @Produces("application/json")
    public String getByKeysJson(@PathParam("starttime") String starttime,
			    @PathParam("endtime") String endtime) {
	Collection<ClientTrace> list = ClientTraceHome.findBetween(starttime,endtime);
	return convertToJson(list);
    }
    @GET
    @Path("starttime/{starttime}/endtime/{endtime}")
    @Produces({"text/plain", "text/csv"})
    public String getByKeysCsv(@PathParam("starttime") String starttime,
			    @PathParam("endtime") String endtime) {
	Collection<ClientTrace> list = ClientTraceHome.findBetween(starttime,endtime);
	return convertToCsv(list);
    }
}

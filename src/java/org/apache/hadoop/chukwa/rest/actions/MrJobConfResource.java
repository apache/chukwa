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

import org.apache.hadoop.chukwa.rest.objects.MrJobConf;
import org.apache.hadoop.chukwa.rest.services.MrJobConfHome;

@Path ("/mr_job_conf")
@Produces("application/xml")
public class MrJobConfResource extends RestController {

    // get one object
    @GET
    @Path("job_id/{job_id}")
    @Produces({"application/xml","text/xml"})
    public String getByJob_IdXML( @PathParam ("job_id") String job_id) {
	MrJobConf model = MrJobConfHome.find(job_id);
	return convertToXml(model);
    }
    
    @GET
    @Path("job_id/{job_id}")
    @Produces("application/json")
    public String getByJob_IdJason( @PathParam ("job_id") String job_id) {
	MrJobConf model = MrJobConfHome.find(job_id);
	return convertToJson(model);
    }
    
    @GET
    @Path("job_id/{job_id}")
    @Produces({"text/plain","text/csv"})
    public String getByJob_IdCsv( @PathParam ("job_id") String job_id) {
	MrJobConf model = MrJobConfHome.find(job_id);
	return convertToCsv(model);
    }
}

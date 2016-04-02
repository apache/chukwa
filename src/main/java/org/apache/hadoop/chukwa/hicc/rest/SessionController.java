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
package org.apache.hadoop.chukwa.hicc.rest;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

@Path("session")
public class SessionController {
  static Logger LOG = Logger.getLogger(SessionController.class);

  /**
   * Utility to get session attributes
   * @param request is HTTP request object
   * @param id is session key
   * @return session attribute
   */
  @GET
  @Path("key/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public String draw(@Context HttpServletRequest request, @PathParam("id") String id) {
    String value = (String) request.getSession().getAttribute(id);
    Map<String, String> map = new HashMap<String, String>();
    map.put(id, value);
    Gson gson = new Gson();
    String json = gson.toJson(map);
    return json;
  }

  @PUT
  @Path("save")
  public Response save(@Context HttpServletRequest request, String buffer) {
    Gson gson = new Gson();
    Type stringStringMap = new TypeToken<Map<String, String>>(){}.getType();
    Map<String,String> map = gson.fromJson(buffer, stringStringMap);
    for(Entry<String, String> entry : (Set<Map.Entry<String, String>>) map.entrySet()) {
      request.getSession().setAttribute(entry.getKey(), entry.getValue());
    }
    return Response.ok().build();
  }
}

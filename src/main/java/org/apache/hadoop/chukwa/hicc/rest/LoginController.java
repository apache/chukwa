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

import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import com.sun.jersey.api.client.ClientResponse.Status;

@Path("/login")
public class LoginController {
  @Context
  VelocityEngine velocity;
  
  static {
    ChukwaHBaseStore.populateDefaults();
  }

  @GET
  @Path("check")
  public String login(String buffer) {
    VelocityContext context = new VelocityContext();
    StringWriter sw = null;
    try {
      Template template = velocity.getTemplate("login.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }

  @POST
  @Path("check")
  public Response check(@Context HttpServletRequest request) {
    VelocityContext context = new VelocityContext();
    if(request.getRemoteUser()!=null) {
      URI location;
      try {
        location = new URI("/hicc/");
        return Response.temporaryRedirect(location).build();
      } catch (URISyntaxException e) {
      }
    }
    context.put("invalid", true);
    Template template = velocity.getTemplate("login.vm");
    StringWriter sw = new StringWriter();
    template.merge(context, sw);
    return Response.status(Status.FORBIDDEN).entity(sw.toString()).build();
  }
}

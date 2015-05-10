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

import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Scope;
import javax.inject.Singleton;
import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.bean.Widget;
import org.apache.log4j.Logger;

import com.google.gson.Gson;

@Path("widget")
public class WidgetController {

  static Logger LOG = Logger.getLogger(WidgetController.class);

  @Context
  private ServletContext context;

  @PostConstruct
  @Singleton
  public void init() {
    ChukwaHBaseStore.populateDefaults();
  }

  @GET
  @Path("list")
  public String listWidget(@DefaultValue("1000") @QueryParam("limit") int limit, 
      @DefaultValue("0") @QueryParam("offset") int offset) {
    List<Widget> widgets = ChukwaHBaseStore.listWidget(limit, offset);
    Gson gson = new Gson();
    String json = gson.toJson(widgets);
    return json;
  }

  @GET
  @Path("search/{query}")
  public String searchWidget(@PathParam("query") String query) {
    List<Widget> widgets = ChukwaHBaseStore.searchWidget(query);
    Gson gson = new Gson();
    String json = gson.toJson(widgets);
    return json;
  }

  @GET
  @Path("view/{title}")
  public String viewWidget(@PathParam("title") String title) {
    Widget w = ChukwaHBaseStore.viewWidget(title);
    Gson gson = new Gson();
    String json = gson.toJson(w);
    return json;
  }

  @POST
  @Path("create")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createWidget(String buffer) {
    Gson gson = new Gson();
    Widget widget = gson.fromJson(buffer, Widget.class);
    boolean result = ChukwaHBaseStore.createWidget(widget);
    if(!result) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

  @PUT
  @Path("update/{title}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response updateWidget(@PathParam("title") String title, String buffer){
    Gson gson = new Gson();
    Widget widget = gson.fromJson(buffer, Widget.class);
    boolean result = ChukwaHBaseStore.updateWidget(title, widget);
    if(!result) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

  @DELETE
  @Path("delete/{title}")
  public Response deleteWidget(@PathParam("title") String title) {
    boolean result = ChukwaHBaseStore.deleteWidget(title);
    if(!result) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }
}

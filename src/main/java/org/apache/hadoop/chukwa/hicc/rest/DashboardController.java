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
import java.util.Set;
import java.net.InetAddress;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.bean.Dashboard;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import com.google.gson.Gson;

@Path("dashboard")
public class DashboardController {
  static Logger LOG = Logger.getLogger(DashboardController.class);

  @Context
  private ServletContext context;
  @Context
  VelocityEngine velocity;

  
  @GET
  @Path("load/{id}")
  public String load(@Context HttpServletRequest request, @PathParam("id") String id) {
    Gson gson = new Gson();
    Dashboard dash = ChukwaHBaseStore.getDashboard(id, request.getRemoteUser());
    String json = gson.toJson(dash);
    return json;
  }
  
  @PUT
  @Path("save/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response save(@Context HttpServletRequest request, @PathParam("id") String id, String buffer) {
    Gson gson = new Gson();
    Dashboard dash = gson.fromJson(buffer, Dashboard.class);
    boolean result = ChukwaHBaseStore.updateDashboard(id, request.getRemoteUser(), dash);
    if(!result) {
      return Response.status(Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }
  
  @GET
  @Path("whoami")
  public String whoami(@Context HttpServletRequest request) {
    return request.getRemoteUser();
  }
  
  @GET
  @Path("quicklinks")
  @Produces(MediaType.TEXT_HTML)
  public String quicklinks() {
    VelocityContext context = new VelocityContext();
    StringWriter sw = null;
    Configuration hconf = HBaseConfiguration.create();
    Configuration hadoop = new Configuration();
    String nn = "";
    String rm = "";
    String hm = "";
    Set<String> sourceNames = ChukwaHBaseStore.getSourceNames("");
    for (String source : sourceNames) {
      String[] sourceParts = source.split(":");
      if(sourceParts.length<2) {
        continue;
      }
      if(sourceParts[1].equals("NameNode")) {
        String[] parts = hadoop.get(DFSConfigKeys.DFS_NAMENODE_HTTP_ADDRESS_KEY).split(":");
        StringBuilder buffer = new StringBuilder();
        try {
          InetAddress address = InetAddress.getByName(sourceParts[0]);
          buffer.append(address.getHostAddress());
        } catch (Exception e) {
          buffer.append(sourceParts[0]);
        } 
        buffer.append(":");
        buffer.append(parts[1]);
        nn = buffer.toString();
      } else if(sourceParts[1].equals("ResourceManager")) {
        String[] parts = hadoop.get(YarnConfiguration.RM_WEBAPP_ADDRESS).split(":");
        StringBuilder buffer = new StringBuilder();
        try {
          InetAddress address = InetAddress.getByName(sourceParts[0]);
          buffer.append(address.getHostAddress());
        } catch (Exception e) {
          buffer.append(sourceParts[0]);
        } 
        buffer.append(":");
        buffer.append(parts[1]);
        rm = buffer.toString();
      } else if(sourceParts[1].equals("Master")) {
        StringBuilder buffer = new StringBuilder();
        try {
          InetAddress address = InetAddress.getByName(sourceParts[0]);
          buffer.append(address.getHostAddress());
        } catch (Exception e) {
          buffer.append(sourceParts[0]);
        } 
        buffer.append(":");
        buffer.append(hconf.getInt("hbase.master.info.port", HConstants.DEFAULT_MASTER_INFOPORT));
        hm = buffer.toString();
      }
    }
    try {
      context.put("nn", nn);
      context.put("rm", rm);
      context.put("hm", hm);
      Template template = velocity.getTemplate("quick-links.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }
}

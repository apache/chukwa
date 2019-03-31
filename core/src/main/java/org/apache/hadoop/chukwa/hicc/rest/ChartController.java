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
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.TimeHandler;
import org.apache.hadoop.chukwa.hicc.bean.Chart;
import org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.jersey.api.Responses;

@Path("/chart")
public class ChartController {
  static Logger LOG = Logger.getLogger(ChartController.class);
  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  @Context
  VelocityEngine velocity;
  
  /**
   * Render chart using flot.js
   * 
   * @param id Reference ID of Chart stored in HBase chukwa_meta table.
   * @return chart widget
   * 
   */
  @GET
  @Path("draw/{id}")
  @Produces(MediaType.TEXT_HTML)
  public String draw(@PathParam("id") String id) {
    VelocityContext context = new VelocityContext();
    StringWriter sw = null;
    try {
      Chart chart = ChukwaHBaseStore.getChart(id);
      List<SeriesMetaData> series = chart.getSeries();
      Gson gson = new Gson();
      String seriesMetaData = gson.toJson(series);

      context.put("chart", chart);
      context.put("seriesMetaData", seriesMetaData);
      Template template = velocity.getTemplate("chart.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }

  /**
   * Describe chart meta data
   * 
   * @param id Chart ID
   * @return chart meta data
   * 
   * @response.representation.200.doc Display chart configuration options
   * @response.representation.200.mediaType application/json
   * @response.representation.200.example {@link Examples#CPU_UTILIZATION}
   * 
   */
  @GET
  @Path("describe/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public String describe(@PathParam("id") String id) {
    Chart chart = ChukwaHBaseStore.getChart(id);
    Gson gson = new Gson();
    String buffer = gson.toJson(chart);
    return buffer;
  }

  /**
   * Create a new chart meta data
   * 
   * @param buffer holds incoming JSON of Chart object
   * @return Web response code
   * 
   * @request.representation.example {@link Examples#MEMORY_UTILIZATION}
   * 
   */
  @POST
  @Path("save")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response create(String buffer) {
    Gson gson = new Gson();
    Chart chart = gson.fromJson(buffer, Chart.class);
    String id = ChukwaHBaseStore.createChart(chart);
    if (id != null) {
      return Response.ok(id).build();
    }
    return Responses.notAcceptable().build();
  }

  /**
   * Save chart meta data
   * 
   * @param id is unique identifier of Chart object
   * @param buffer holds incoming JSON of Chart object
   * @return Web response code
   * 
   * @request.representation.example {@link Examples#DISK_UTILIZATION}
   * 
   */
  @PUT
  @Path("save/{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response save(@PathParam("id") String id, String buffer) {
    Gson gson = new Gson();
    Chart chart = gson.fromJson(buffer, Chart.class);
    ChukwaHBaseStore.putChart(id, chart);
    return Response.ok().build();
    
  }

  /**
   * Display a chart base on chart configuration from REST API input
   * 
   * @param buffer holds incoming JSON of Chart object
   * @return segment of chart HTML output
   * 
   * @request.representation.example {@link Examples#NETWORK_UTILIZATION}
   *
   */
  @PUT
  @Path("preview")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.TEXT_HTML)
  public String preview(String buffer) {
    VelocityContext context = new VelocityContext();
    StringWriter sw = null;
    try {
      Gson gson = new Gson();
      Chart chart = gson.fromJson(buffer, Chart.class);
      List<SeriesMetaData> series = chart.getSeries();
      String seriesMetaData = gson.toJson(series);

      context.put("chart", chart);
      context.put("seriesMetaData", seriesMetaData);
      Template template = velocity.getTemplate("chart.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }

  /**
   * Display metrics series in JSON
   * 
   * @param request HTTP request object
   * @param buffer list of SeriesMetaData
   * @return metrics JSON
   * 
   * @request.representation.example {@link Examples#CPU_SERIES_METADATA}
   * @response.representation.200.doc Display series data in JSON
   * @response.representation.200.mediaType application/json
   * 
   */
  @PUT
  @Path("preview/series")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces("application/json")
  public String previewSeries(@Context HttpServletRequest request, String buffer) {
    Type listType = new TypeToken<ArrayList<SeriesMetaData>>() {
    }.getType();
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    startTime = time.getStartTime();
    endTime = time.getEndTime();
    Gson gson = new Gson();
    ArrayList<SeriesMetaData> series = gson.fromJson(buffer, listType);
    series = ChukwaHBaseStore.getChartSeries(series, startTime, endTime);
    String result = gson.toJson(series);
    return result;
  }

}

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
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

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

@Path("/tile")
public class TileController extends ChartController{
  static Logger LOG = Logger.getLogger(ChartController.class);
  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  @Context
  VelocityEngine velocity;

  /**
   * Render a banner
   * 
   * @param id Reference ID of Chart stored in HBase chukwa_meta table.
   * @return html chart widget
   * 
   * @response.representation.200.doc Render a banner base on chart id
   * @response.representation.200.mediaType text/html
   * @response.representation.200.example Example available in HICC UI
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
      Template template = velocity.getTemplate("tile.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }

  /**
   * Preview a banner tile
   * 
   * @param buffer is tile object in JSON
   * @return html for rendering a banner tile
   * 
   * @response.representation.200.doc Preview a banner
   * @response.representation.200.mediaType text/html
   * @response.representation.200.example Example available in HICC UI
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
      Template template = velocity.getTemplate("tile.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }

  /**
   * Preview a series JSON for banner tile
   * 
   * @param request HTTP request object
   * @param buffer is banner tile configuration
   * 
   * @request.representation.example {@link Examples#CPU_SERIES_METADATA}
   * @response.representation.200.doc Preview a banner series
   * @response.representation.200.mediaType application/json
   * @response.representation.200.example Example available in REST API
   */
  @PUT
  @Path("preview/series")
  @Produces(MediaType.APPLICATION_JSON)
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
    List<String> data = ChukwaHBaseStore.getData(series, startTime, endTime);
    String result = gson.toJson(data);
    return result;
  }
}

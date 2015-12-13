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
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.TimeHandler;
import org.apache.hadoop.chukwa.hicc.bean.Chart;
import org.apache.hadoop.chukwa.hicc.bean.Series;
import org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

@Path("/circles")
public class CirclesController {
  static Logger LOG = Logger.getLogger(CirclesController.class);
  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

  @Context
  VelocityEngine velocity;
  
  /**
   * Render circle using jquery circliful.js
   * 
   * @param id Chart id
   * @param invert Toggle to display warning, error color by upper bound or lower bound.
   * @return html circle widget.
   */
  @GET
  @Path("draw/{id}")
  @Produces(MediaType.TEXT_HTML)
  public String draw(@PathParam("id") String id, @DefaultValue("false") @QueryParam("invert") boolean invert) {
    VelocityContext context = new VelocityContext();
    StringWriter sw = null;
    try {
      Chart chart = ChukwaHBaseStore.getChart(id);
      List<SeriesMetaData> series = chart.getSeries();
      Gson gson = new Gson();
      String seriesMetaData = gson.toJson(series);

      context.put("chart", chart);
      context.put("seriesMetaData", seriesMetaData);
      Template template = velocity.getTemplate("circles.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }

  @PUT
  @Path("preview")
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
      Template template = velocity.getTemplate("circles.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }
  
  @PUT
  @Path("preview/series")
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
    double percent;
    List<String> data = ChukwaHBaseStore.getData(series, startTime, endTime);
    if(series.size()>=2) {
      double x = 0;
      double y = 1;
      try {
        x = Double.parseDouble(data.get(0));
      } catch(NumberFormatException e) {
        x = 0;
      }
      try {
        y = Double.parseDouble(data.get(1));
      } catch(NumberFormatException e) {
        y = 1;
      }
      percent = x / y * 100d;
    } else {
      double x = Double.parseDouble(data.get(0));
      percent = x;
    }
    percent = Math.round(percent * 100d) / 100d;
    Series answer = new Series("circle");
    answer.add(endTime, percent);
    String result = gson.toJson(answer);
    return result;
  }
}

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
import java.text.SimpleDateFormat;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.TimeHandler;
import org.apache.hadoop.chukwa.hicc.bean.Heatmap;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

@Path("/heatmap")
public class HeatmapController {
  static Logger log = Logger.getLogger(HeatmapController.class);
  
  @Context
  VelocityEngine velocity;

  /**
   * Render a heatmap
   * @param request is HTTP request object
   * @param metricGroup is metric group name
   * @param metric is metric name
   * @param start is start time in yyyyMMddHHmmss format
   * @param end is end time in yyyyMMddHHmmss format
   * @param max is maximum possible value of the heatmap
   * @param scale is the range of possible values
   * @param width is width of the image
   * @param height is height of the image
   * @return html page of login screen
   * 
   * @response.representation.200.doc Login screen
   * @response.representation.200.mediaType text/html
   * @response.representation.200.example Example available in HICC UI
   */

  @GET
  @Path("{metricGroup}/{metric}")
  @Produces(MediaType.APPLICATION_JSON)
  public Heatmap getHeatmap(@Context HttpServletRequest request,
      @PathParam("metricGroup") String metricGroup,
      @PathParam("metric") String metric, @QueryParam("start") String start,
      @QueryParam("end") String end,
      @QueryParam("max") @DefaultValue("1.0") double max,
      @QueryParam("scale") @DefaultValue("100") double scale,
      @QueryParam("width") @DefaultValue("700") int width,
      @QueryParam("height") @DefaultValue("400") int height) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    Heatmap heatmap = null;
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    try {
      if (start != null) {
        startTime = sdf.parse(start).getTime();
      } else {
        startTime = time.getStartTime();
      }
      if (end != null) {
        endTime = sdf.parse(end).getTime();
      } else {
        endTime = time.getEndTime();
      }
      heatmap = ChukwaHBaseStore.getHeatmap(metricGroup, metric, startTime,
          endTime, max, scale, width, height);
    } catch (Throwable e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return heatmap;
  }
  
  /**
   * Render a heatmap from HBase series
   * 
   * @param metricGroup is metric group name
   * @param metric is metric name
   * @param width is width of the image
   * @param height is height of the image
   * @param title is title of the heatmap
   * @param yLabel is y axis label for the heatmap
   * @return heatmap chart in html
   */
  @GET
  @Path("render/{metricGroup}/{metric}")
  @Produces(MediaType.TEXT_HTML)
  public String heatmapTemplate(@PathParam("metricGroup") @DefaultValue("SystemMetrics") String metricGroup,
      @PathParam("metric") @DefaultValue("cpu.combined.") String metric,
      @QueryParam("width") @DefaultValue("700px") String width,
      @QueryParam("height") @DefaultValue("400px") String height,
      @QueryParam("title") @DefaultValue("CPU") String title,
      @QueryParam("yLabel") @DefaultValue("device") String yLabel) {
    StringBuilder url = new StringBuilder();
    url.append("/hicc/v1/heatmap/").append(metricGroup).append("/").append(metric);
    VelocityContext context = new VelocityContext();
    StringWriter sw = null;
    try {
      context.put("url", url.toString());
      context.put("width", width);
      context.put("height", height);
      context.put("title", title);
      context.put("yLabel", yLabel);
      Template template = velocity.getTemplate("heatmap.vm");
      sw = new StringWriter();
      template.merge(context, sw);
    } catch (Exception e) {
      e.printStackTrace();
      return e.getMessage();
    }
    return sw.toString();
  }
}

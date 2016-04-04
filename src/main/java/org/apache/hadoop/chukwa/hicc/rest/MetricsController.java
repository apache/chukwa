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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.apache.hadoop.chukwa.datacollection.agent.rest.Examples;
import org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore;
import org.apache.hadoop.chukwa.hicc.TimeHandler;
import org.apache.hadoop.chukwa.hicc.bean.Series;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

@Path("/metrics")
public class MetricsController {

  /**
   * Query metrics stored in HBase table
   * 
   * @param request is HTTP request object
   * @param metric is metric name
   * @param source is data source
   * @param start is start time
   * @param end is end time
   * @return Metrics JSON
   * 
   */
  @GET
  @Path("series/{metric}/{source}")
  @Produces("application/json")
  public String getSeries(@Context HttpServletRequest request, @PathParam("metric") String metric, @PathParam("source") String source, @QueryParam("start") String start, @QueryParam("end") String end) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String buffer = "";
    Series series;
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    try {
      if(start!=null) {
        startTime = sdf.parse(start).getTime();
      } else {
        startTime = time.getStartTime();
      }
      if(end!=null) {
        endTime = sdf.parse(end).getTime();
      } else {
        endTime = time.getEndTime();
      }
      series = ChukwaHBaseStore.getSeries(metric, source, startTime, endTime);
      buffer = series.toString();
    } catch (ParseException e) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
          .entity("Start/End date parse error.  Format: yyyyMMddHHmmss.").build());
    }
    return buffer;
  }

  @GET
  @Path("series/{metricGroup}/{metric}/session/{sessionKey}")
  @Produces("application/json")
  public String getSeriesBySessionAttribute(@Context HttpServletRequest request, @PathParam("metricGroup") String metricGroup, @PathParam("metric") String metric, @PathParam("sessionKey") String skey, @QueryParam("start") String start, @QueryParam("end") String end) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
    String buffer = "";
    long startTime = 0;
    long endTime = 0;
    TimeHandler time = new TimeHandler(request);
    try {
      if(start!=null) {
        startTime = sdf.parse(start).getTime();
      } else {
        startTime = time.getStartTime();
      }
      if(end!=null) {
        endTime = sdf.parse(end).getTime();
      } else {
        endTime = time.getEndTime();
      }
      if(skey!=null) {
          HttpSession session = request.getSession();
          String[] sourcekeys = (session.getAttribute(skey).toString()).split(",");
          Type seriesListType =new TypeToken<ArrayList<Series>>(){}.getType();
          ArrayList<Series> seriesList = new ArrayList<Series>();
          for(String source : sourcekeys) {
            if (source == null || source.equals("")) {
              continue;
            }
            Series output = ChukwaHBaseStore.getSeries(metricGroup, metric, source, startTime, endTime);
            seriesList.add(output);
          }
          buffer = new Gson().toJson(seriesList, seriesListType);
      } else {
        throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
            .entity("No session attribute key defined.").build());
      }
    } catch (ParseException e) {
      throw new WebApplicationException(Response.status(Response.Status.BAD_REQUEST)
          .entity("Start/End date parse error.  Format: yyyyMMddHHmmss.").build());
    }
    return buffer;
  }

  @GET
  @Path("schema")
  @Produces("application/json")
  public String getTables() {
    Set<String> metricGroups = ChukwaHBaseStore.getMetricGroups();
    Type metricGroupsType = new TypeToken<List<String>>(){}.getType();
    String groups = new Gson().toJson(metricGroups, metricGroupsType);
    return groups;
  }
  
  @GET
  @Path("schema/{metricGroup}")
  @Produces("application/json")
  public String getMetrics(@PathParam("metricGroup") String metricGroup) {
    Set<String> metricNames = ChukwaHBaseStore.getMetricNames(metricGroup);
    Type metricsType = new TypeToken<List<String>>(){}.getType();
    String metrics = new Gson().toJson(metricNames, metricsType);
    return metrics;
  }

  @GET
  @Path("source/{metricGroup}")
  @Produces("application/json")
  public String getSourceNames(@Context HttpServletRequest request, @PathParam("metricGroup") String metricGroup) {
    Set<String> sourceNames = ChukwaHBaseStore.getSourceNames(metricGroup);
    Type rowsType = new TypeToken<List<String>>(){}.getType();
    String rows = new Gson().toJson(sourceNames, rowsType);
    return rows;
  }

}

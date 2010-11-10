<%
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
%>
<%@ page import = "java.sql.*" %>
<%@ page import = "java.io.*" %>
<%@ page import = "java.util.Calendar" %>
<%@ page import = "java.util.Date" %>
<%@ page import = "java.text.SimpleDateFormat" %>
<%@ page import = "java.util.*" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.ClusterConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.Chart" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.DatasetMapper" %>
<%@ page import = "org.apache.hadoop.chukwa.database.DatabaseConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.database.Macro" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%
    XssFilter xf = new XssFilter(request);
    //String boxId=xf.getParameter("boxId");
    //response.setHeader("boxId", xf.getParameter("boxId"));
    response.setContentType("text/html; chartset=UTF-8//IGNORE");

    /*
     * Set chart title, and output type.
     */
    String title = xf.getParameter("title");
    String graphType = xf.getParameter("graph_type");

    /*
     * Set chart width.
     */
    int width=300;
    if(request.getParameter("width")!=null) {
      width=Integer.parseInt(request.getParameter("width"));
    }

    /*
     * Set chart height.
     */
    int height=200;
    if(request.getParameter("height")!=null) {
      height=Integer.parseInt(request.getParameter("height"));
    }

    /*
     * Set series data source.
     */
    String[] seriesName = xf.getParameterValues("series_name");
    String[] data = xf.getParameterValues("data");
    if(xf.getParameterValues("data").length==1 && xf.getParameterValues("data")[0].indexOf(",")!=-1) {
      data = xf.getParameterValues("data")[0].split(",");
    }
    
    /*
     * Set series render format.
     */
    String[] render = null;
    if(request.getParameter("render")!=null) {
      render=xf.getParameterValues("render");
    } 
    if(render==null || render.length!=data.length) { 
      render = new String[data.length];
      for(int i=0;i<data.length;i++) {
        render[i] = "line";
      }
    }

    Chart c = new Chart(request);

    /*
     * Setup x axis display.
     */
    if(request.getParameter("x_label")!=null && xf.getParameter("x_label").equals("on")) {
      c.setXAxisLabels(true);
    } else {
      c.setXAxisLabels(false);
    }
    if(request.getParameter("x_axis_label")!=null) {
      c.setXAxisLabel(xf.getParameter("x_axis_label"));
    } else {
      c.setXAxisLabel("Time");
    }

    // set ymin and ymax
    // TODO: check that these are numbers
    if (request.getParameter("ymin") != null && 
        request.getParameter("ymin").length() != 0) {
      c.setYMin(new Double(xf.getParameter("ymin")));
    }
    if (request.getParameter("ymax") != null &&
        request.getParameter("ymax").length() != 0) {
      c.setYMax(new Double(xf.getParameter("ymax")));
    }

    /*
     * Setup y axis display.
     */
    if(request.getParameter("y_label")!=null && xf.getParameter("y_label").equals("on")) {
      c.setYAxisLabels(true);
      c.setYAxisLabel(xf.getParameter("y_axis_label"));
    } else {
      c.setYAxisLabels(false);
      c.setYAxisLabel("");
    }
    if(request.getParameter("y_axis_max")!=null) {
      double max = Double.parseDouble(xf.getParameter("y_axis_max"));
      c.setYMax(max);
    }
    if(request.getParameter("display_percentage")!=null) {
      c.setDisplayPercentage(true);
    }

    /*
     * Setup title.
     */
    if(title!=null) {
      c.setTitle(title);
    }

    /*
     * Setup legend display.
     */
    if(request.getParameter("legend")!=null && xf.getParameter("legend").equals("off")) {
      c.setLegend(false);
    }

    /*
     * Setup series display.
     */
    c.setGraphType(graphType);
    c.setSize(width,height);

    /*
     * Setup data structure.
     */
    if(seriesName!=null && data.length==seriesName.length) {
      for(int i=0;i<data.length;i++) {
        c.setDataSet(render[i],seriesName[i],data[i]);
      }
    } else {
      for(int i=0;i<data.length;i++) {
        c.setDataSet(render[i],"",data[i]);
      }
    }

    /*
     * Render graph
     */
    out.println(c.plot());
%>

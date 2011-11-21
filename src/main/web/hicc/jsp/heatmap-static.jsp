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
<%@ page import = "org.apache.hadoop.chukwa.analysis.salsa.visualization.Heatmap" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.ImageSlicer" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%@ page import = "org.apache.hadoop.chukwa.util.ExceptionUtil" %>
<%@ page import = "java.io.FileOutputStream" %>
<%@ page import = "java.io.File" %>
<%@ page import = "java.util.HashMap" %>
<%@ page import = "java.util.HashMap" %>
<%@ page import = "org.apache.commons.logging.Log" %>
<%@ page import = "org.apache.commons.logging.LogFactory" %>
<%
   Log log = LogFactory.getLog(Heatmap.class);
   XssFilter xf = new XssFilter(request);
   String boxId = xf.getParameter("boxId");
   int max = 0;
   try {
     StringBuilder fileName = new StringBuilder();
     fileName.append(System.getenv("CHUKWA_HOME"));
     fileName.append(File.separator);
     fileName.append("webapps");
     fileName.append(File.separator);
     fileName.append("sandbox");
     fileName.append(File.separator);
     fileName.append(boxId);
     fileName.append(xf.getParameter("_s"));
     fileName.append(".png");
     StringBuilder baseFileName = new StringBuilder();
     baseFileName.append(boxId);
     baseFileName.append(xf.getParameter("_s"));
     baseFileName.append(".png");
 
     FileOutputStream fos = new FileOutputStream(fileName.toString());

     Heatmap sw = new Heatmap(request);
     sw.setDimensions(2000,2000);
     sw.run();
     if(sw.getImage(fos,"PNG", 1.0)) {
       fos.close();
       ImageSlicer slicer = new ImageSlicer();
       max = slicer.process(baseFileName.toString());
     }
     RequestDispatcher disp = getServletContext().getRequestDispatcher("/jsp/image-viewer.jsp");
     request.setAttribute("image-viewer",baseFileName.toString());
     request.setAttribute("maxLevel",max);
     disp.forward(request, response);
   } catch(Exception e) {
     response.setHeader("boxId", xf.getParameter("boxId"));
     response.setContentType("text/html; chartset=UTF-8//IGNORE");
     out.println("No data available.");
     log.error(ExceptionUtil.getStackTrace(e));
   }
%>

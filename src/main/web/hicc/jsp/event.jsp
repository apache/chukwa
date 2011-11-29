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
<%@ page import = "java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<% TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
   long start = time.getStartTime();
   long end = time.getEndTime();
   long midpoint = (end+start)/2;
   SimpleDateFormat formatter = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
   String startDate = formatter.format(midpoint);
   String endDate = formatter.format(end);
   String intervalUnit1="MINUTE";
   String intervalUnit2="HOUR";
   int intervalPixels = 10;
   if(((end-start)/1000)>=(60*60*24*3)) {
       intervalUnit1 = "DAY";
       intervalUnit2 = "WEEK";
       intervalPixels = 600;
       if(((end-start)/1000)>(60*60*24*15)) {
         intervalPixels = 300;
       }
   } else if(((end-start)/1000)>(60*60*6)) {
       intervalUnit1 = "HOUR";
       intervalUnit2 = "DAY";
       intervalPixels = 600;
   } else {
       intervalUnit1 = "MINUTE";
       intervalUnit2 = "HOUR";
       intervalPixels = 600;
       if(((end-start)/1000)>(60*60*3)) {
         intervalPixels = 250;
       }
   }
%>
<html>
  <head>
    <link rel='stylesheet' href='/hicc/lib/timeline/bundle.css' type='text/css' />
    <script src="/hicc/lib/timeline/timeline-api.js?bundle=true" type="text/javascript"></script>
    <script src="/hicc/lib/timeline/search.js" type="text/javascript"></script>
    <script type="text/javascript">
        var theme = Timeline.ClassicTheme.create();
        theme.event.label.width = 220; // px
        theme.event.bubble.width = 400;
        theme.event.bubble.height = 80;
        function onLoad() {
          var eventSource = new Timeline.DefaultEventSource();
          var bandInfos = [
            Timeline.createBandInfo({
                eventSource:    eventSource,
                showEventText:  false,
                trackHeight:    0.5,
                trackGap:       0.2,
                date:           "<%= startDate %>  GMT",
                width:          "100%", 
                intervalUnit:   Timeline.DateTime.<%= intervalUnit2 %>, 
                intervalPixels: <%= intervalPixels %>,
                theme: theme,
            })
          ];
          bandInfos[0].highlight = true;
  
          tl = Timeline.create(document.getElementById("my-timeline"), bandInfos);
          Timeline.loadXML("events-xml.jsp", function(xml, url) { eventSource.loadXML(xml, url); });
          setupFilterHighlightControls(document.getElementById("controls"), tl, [0], theme);

        }
        var resizeTimerID = null;
        function onResize() {
            if (resizeTimerID == null) {
                resizeTimerID = window.setTimeout(function() {
                    resizeTimerID = null;
                    tl.layout();
                }, 500);
            }
        }
    </script>
  </head>
  <body onload="onLoad();" onresize="onResize();">
    <div id="my-timeline" style="height: 500px; border: 1px solid #aaa"></div>
    <div class="controls" id="controls">
    </div>
  </body>
</html>

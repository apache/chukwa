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
<%@ page import = "java.util.Hashtable, java.util.StringTokenizer, java.util.Enumeration, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.TimeHandler, java.text.NumberFormat, org.apache.hadoop.chukwa.util.XssFilter" %>
<%
   XssFilter xf = new XssFilter(request);
   response.setHeader("boxId", xf.getParameter("boxId"));
%>
<% String boxId = xf.getParameter("boxId"); 
   TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone")); %>
Time Period 
<select class="timeWidgetPeriodControl" id="<%= boxId %>period" name="<%= boxId %>time_period" class="formSelect">
<%
   boolean fCustom=false;

   String period = (String) session.getAttribute("period");
   String[][] periodMap = new String[9][2];
   periodMap[0][0]="last1hr";
   periodMap[0][1]="Last 1 Hour";
   periodMap[1][0]="last2hr";
   periodMap[1][1]="Last 2 Hours";
   periodMap[2][0]="last3hr";
   periodMap[2][1]="Last 3 Hours";
   periodMap[3][0]="last6hr";
   periodMap[3][1]="Last 6 Hours";
   periodMap[4][0]="last12hr";
   periodMap[4][1]="Last 12 Hours";
   periodMap[5][0]="last24hr";
   periodMap[5][1]="Last 24 Hours";
   periodMap[6][0]="last7d";
   periodMap[6][1]="Last 7 Days";
   periodMap[7][0]="last30d";
   periodMap[7][1]="Last 30 Days";
   periodMap[8][0]="custom";
   periodMap[8][1]="Custom Period";
   for (int i=0;i<periodMap.length;i++) {
       String meta = "";
       if (period!=null && period.equals(periodMap[i][0])) {
           meta = "selected";
       } else if (period.startsWith("custom;") && (periodMap[i][0].equals("custom"))) {
           meta = "selected";
	   fCustom = true;       	   
       }

       out.println("<option value='"+periodMap[i][0]+"' "+meta+">"+periodMap[i][1]+"</option>");
   }
 %>
</select>
<div id="<%= boxId %>period_custom_block" style="display:<%= (fCustom?"block":"none") %>;">
<%
    String start_string="2 days ago";
    String end_string="now";
    if (period.startsWith("custom;")) {
      // the string should be custom;<start>;<end>
      StringTokenizer st=new StringTokenizer(period,";");
      if (st.hasMoreTokens()) {
        st.nextToken(); // skip custom;
	if (st.hasMoreTokens()) {
	   start_string=st.nextToken();
	   if (st.hasMoreTokens()) {
	      end_string=st.nextToken();
	   }
	}
      }
    }
%>
    <br/>
    <fieldset>
    <legend>Custom Period</legend>
    <table>
    <tr><td>
    <label for="start_period">Start Period:</label>
    </td><td>
    <input type="edit" name="<%= boxId %>_start_time" id="<%= boxId %>_start_time" value="<%= start_string %>"/>
    </td><td>
    <a href="#" id="help_edit_start_time" class="<%= boxId %>help_control">?</a>
    </td></tr>
    <tr><td>
    <label for="end_period">End Period:</label>
    </td><td>
    <input type="edit" name="<%= boxId %>_end_time" id="<%= boxId %>_end_time" value="<%= end_string %>"/>
    </td><td>
    <a href="#" id="help_edit_start_time" class="<%= boxId %>help_control">?</a>
    </td></tr>
    </table>
    </fieldset>

    </div>
<input type="button" name="<%= boxId %>apply" value="Apply" onclick="save_time_range('<%= boxId %>')" class="formButton">

<script type='text/javascript' src="/hicc/js/"></script>
<script>
/*
 * popup the online help
 */
function popup_help(event) {
  var element = Event.element(event);
  window.open("/hicc/jsp/help.jsp?id="+element.id,
	      "Help",
	      "width=500,height=400");
}

/*
 * toggle the custom period control
 */
function togglePeriodControl(event) {
  var element = Event.element(event);
  value=$F(element.id);
  if (value=='custom') {
    $(element.id+'_custom_block').show();
  } else {
    $(element.id+'_custom_block').hide();
  }
}


period_control = document.getElementById("<%= boxId %>period");

if (period_control != null) {
   //period_control.observe('change',togglePeriodControl);
   help_controls=document.getElementsByClassName("<%= boxId %>help_control");
   for (i=0;i<help_controls.length;i++) {
       //help_controls[i].observe("click",popup_help);
   }
}
</script>

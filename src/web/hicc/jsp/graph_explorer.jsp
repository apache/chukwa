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
<%@ page import = "java.text.DecimalFormat,java.text.NumberFormat" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>

<%
    XssFilter xf = new XssFilter(request);
    NumberFormat nf = new DecimalFormat("###,###,###,##0.00");
    response.setHeader("boxId", xf.getParameter("boxId"));
    response.setContentType("text/html; chartset=UTF-8//IGNORE");
    String boxId=xf.getParameter("boxId");
    String cluster = (String) session.getAttribute("cluster");
%>
<html>
  <head>
  <script src="/hicc/js/jquery-1.3.2.min.js" type="text/javascript" charset="utf-8"></script>
  <script src="/hicc/js/autoHeight.js" type="text/javascript" charset="utf-8"></script>
  <script>
    $.ajax({ url: "/hicc/v1/metrics/schema", dataType: "json", success: function(data){
      for(var i in data) {
        $('#table').append("<option>"+data[i]+"</option>");
      }
    }});

    function getFamilies() {
      var size = $('#family option').size();
      $('#family').find('option').remove();
      var table = $('#table').val();
      $.ajax({ url: "/hicc/v1/metrics/schema/"+table, dataType: "json", success: function(data){
        for(var i in data) {
          $('#family').append("<option>"+data[i]+"</option>");
        }
      }});
    }

    function getColumns() {
      var size = $('#column option').size();
      $('#column').find('option').remove();
      var table = $('#table').val();
      var family = $('#family').val();
      $('#family :selected').each(function(i, selected) {
        var family = $(selected).val();
        var url = "/hicc/v1/metrics/schema/"+table+"/"+family;
        $.ajax({ url: url, dataType: "json", success: function(data){
          for(var i in data) {
            $('#column').append("<option>"+data[i]+"</option>");
          }
        }});
      });
    }

    function getRows() {
      var size = $('#row option').size();
      $('#row').find('option').remove();
      var table = $('#table').val();
      var family = $('#family').val();
      var column = $('#column').val();
      $('#column :selected').each(function(i, selected) {
        var column = $(selected).val();
        var url = "/hicc/v1/metrics/rowkey/"+table+"/"+column;
        $.ajax({ url: url, dataType: "json", success: function(data){
          for(var i in data) {
            $('#row').not(":contains('"+data[i]+"')").append("<option>"+data[i]+"</option>");
          }
        }});
      });
    }

    function plot() {
      var test = $('#row').val();
      if(test == null) {
        $('#row option:eq(0)').attr('selected',true);
      }
      var data = [];
      $('#column :selected').each(function(i, selected) {
        data[i] = $(selected).val();
      });
      var url = [];
      for(var i in data) {
        url[i] = "/hicc/v1/metrics/series/" + $('#table').val() + "/" + data[i] + "/rowkey/" + $('#row').val();
      } 
      var title = $('#title').val();
      $('#graph').attr('src', "/hicc/jsp/chart.jsp?title="+title+"&data="+url.join("&data="));
      $('#graph').load(function() {
        doIframe();
      });
    }
  </script>
  </head>
  <body>
    <form>
      <center>
      <table>
        <tr>
          <td colspan="3">
          Title <input type=text id="title">
          </td>
        </tr>
        <tr>
          <td>
            Table<br>
            <select id="table" size="10" onMouseUp="getFamilies()">
            </select>
          </td>
          <td>
            Column Family<br>
            <select id="family" multiple size="10" onMouseUp="getColumns()">
            <option>test</option>
            </select>
          </td>
          <td>
            Column<br>
            <select id="column" multiple size="10" onMouseUp="getRows()">
            </select>
          </td>
          <td>
            Row<br>
            <select id="row" size="10">
            </select>
          </td>
        </tr>
        <tr>
          <td>
            <input type=button name="action" value="Plot" onClick="plot()">
          </td>
          <td>
          </td>
          <td>
          </td>
        </tr>
      </table>
    </form>
    <iframe id="graph" width="95%" class="autoHeight" frameBorder="0" scrolling="no"></iframe>
    </center>
  </body>
</html>

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
  <link href="/hicc/css/default.css" rel="stylesheet" type="text/css">
  <link href="/hicc/css/formalize.css" rel="stylesheet" type="text/css">
  <script src="/hicc/js/jquery-1.3.2.min.js" type="text/javascript" charset="utf-8"></script>
  <script src="/hicc/js/jquery.formalize.js"></script>
  <script src="/hicc/js/autoHeight.js" type="text/javascript" charset="utf-8"></script>
  <script>
    function randString(n) {
      if(!n) {
        n = 5;
      }

      var text = '';
      var possible = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

      for(var i=0; i < n; i++) {
        text += possible.charAt(Math.floor(Math.random() * possible.length));
      }

      return text;
    }

    var checkDataLength = function(curOption) {
      return function(data) {
        if (data.length == 0) {
          curOption.attr('disabled', 'disabled');
        }
      }
    };

    $.ajax({ url: "/hicc/v1/metrics/schema", dataType: "json", success: function(data){
      for(var i in data) {
        $('#table').append("<option>"+data[i]+"</option>");
      }
      // Look through each table option and see if it has anything in its family?
      $('#table').children().each(
        function() {
          var table = $(this).text();
          $.ajax({ url: encodeURI("/hicc/v1/metrics/schema/"+table), 
                   dataType: "json", 
                   success: checkDataLength($(this))
          });
        }
      );
    }});


    function getFamilies() {
      var size = $('#family option').size();
      $('#family').find('option').remove();
      var table = $('#table').val();
      $.ajax({ url: encodeURI("/hicc/v1/metrics/schema/"+table), dataType: "json", success: function(data){
        for(var i in data) {
          $('#family').append("<option>"+data[i]+"</option>");
        }
        // Look through each family option and see if it has any columns
        var table = $('#table').val();
        $('#family').children().each(
          function() {
            var family = $(this).text();
            $.ajax({ url: encodeURI("/hicc/v1/metrics/schema/"+table+"/"+family), 
                     dataType: "json", 
                     success: checkDataLength($(this))
            });
          }
        );
      }});
    }

    function getColumns() {
      var size = $('#column option').size();
      $('#column').find('option').remove();
      var table = $('#table').val();
      var family = $('#family').val();
      $('#family :selected').each(function(i, selected) {
        var family = $(selected).val();
        var url = encodeURI("/hicc/v1/metrics/schema/"+table+"/"+family);
        var tableFamily = table+"/"+family;
        // Look through each column option and see if it has any rows
        $.ajax({ url: url, dataType: "json", success: function(data){
          for(var i in data) {
            $('#column').append(
              $("<option></option>")
                .attr("value", tableFamily+"/"+data[i])
                .text(data[i])
            );
          }
        }});
      });
    }

    function getRows() {
      var size = $('#row option').size();
      $('#row').find('option').remove();
      var column = $('#column').val();
      $('#column :selected').each(function(i, selected) {
        var tfColumn = $(selected).val();
        var url = encodeURI("/hicc/v1/metrics/rowkey/"+tfColumn);
        $.ajax({ url: url, dataType: "json", success: function(data){
          for(var i in data) {
            var test = $('#row').find('option[value="'+data[i]+'"]').val();
            if(typeof(test) == "undefined") {
              $('#row').append('<option value="'+data[i]+'">'+data[i]+'</option>');
            }
          }
        }});
      });
    }

    function plot() {
      var test = $('#row').val();
      if(test == null) {
        $('#row option:eq(0)').attr('selected',true);
      }
      var url = [];
      var idx = 0;
      $('#column :selected').each(function(i, selected) {
        $('#row :selected').each(function(j, rowSelected) {
          url[idx++] = encodeURI("/hicc/v1/metrics/series/" + $(selected).val() + "/rowkey/" + $(rowSelected).val());
        }); 
      });
      var title = $('#title').val();
      var ymin = $('#ymin').val();
      var ymax = $('#ymax').val();
      var chart_path = "/hicc/jsp/chart.jsp?title=" + title + "&ymin=" + ymin + "&ymax=" + ymax + "&data=" + url.join("&data=")
      $('#graph').attr('src', encodeURI(chart_path));
      $('#graph').load(function() {
        doIframe();
      });
    }

    function buildWidget() {
      var json = {};
      json.id          = randString(10);
      json.title       = $('#title').val();
      json.version     = "0.1";
      json.categories  = $('#table').val()+","+$("#family").val();
      json.url         = "iframe/jsp/chart.jsp";
      json.description = "User defined widget.";
      json.refresh     = "15";
      json.parameters  = [
       {
         "name"  : "title",
         "type"  : "string",
         "value" : $('#title').val(),
         "edit"  : "1",
         "label" : "Title"
       },
       {
         "name"    : "period",
         "type"    : "custom",
         "control" : "period_control",
         "value"   : "",
         "label"   : "Period"
       },
       {
         "name"    : "width",
         "type"    : "select",
         "value"   : "300",
         "label"   : "Width",
         "options" : [
           {"label":"300","value":"300"},
           {"label":"400","value":"400"},
           {"label":"500","value":"500"},
           {"label":"600","value":"600"},
           {"label":"800","value":"800"},
           {"label":"1000","value":"1000"},
           {"label":"1200","value":"1200"}
         ]
       },
       {
         "name"    : "height",
         "type"    : "select",
         "value"   : "200",
         "label"   : "Height",
         "options" : [
           {"label":"200","value":"200"},
           {"label":"400","value":"400"},
           {"label":"600","value":"600"},
           {"label":"800","value":"800"},
           {"label":"1000","value":"1000"}
         ]
       },
       {
         "name"    : "legend",
         "type"    : "radio",
         "value"   : "on",
         "label"   : "Show Legends",
         "options" : [
           {"label":"On","value":"on"},
           {"label":"Off","value":"off"}
         ]
       }
      ];

      var idx = 0;
      var selections = {};
      selections.name = "data";
      selections.type = "select_multiple";
      selections.label = "Metric";
      selections.options = [];
      selections.value = [];

      var test = $('#row').val();
      if(test == null) {
        $('#row option:eq(0)').attr('selected',true);
      }
      var family = $("#family").val();

      /* loop through series to construct URLs */
      $('#column :selected').each(function(i, selected) {
        var option = {};
        option.label = $('#table').val() + "." + 
          family + "." + 
          $(selected).val() + "." + 
          $('#row').val();
        var values = encodeURI("/hicc/v1/metrics/series/" + 
             $(selected).val() + 
             "/rowkey/" + $('#row').val());
        option.value = values;
        selections.value[idx] = values;
        selections.options[idx++] = option;
      });
      var size = Object.keys(json.parameters).length;
      json.parameters[size++]=selections;
      console.log(JSON.stringify(json));
      if(idx==0) {
        throw "no series selected.";
      }
      return json;
    }

    function exportWidget() {
      var json;
      var url = "/hicc/v1/widget";
      try {
        if($('#title').val()=="") {
          $("#title").val("Please provide a title");
          $("#title").addClass("error");
          $("#title").bind("click", function() {
            $("#title").val("");
            $("#title").removeClass("error");
            $("#title").unbind("click");
          });
          throw "no title provided.";
        }
        json = buildWidget();
      } catch(err) {
        console.log(err);
        return false;
      }
      $.ajax({ 
        type: "PUT",
        url: url, 
        contentType : "application/json",
        data: JSON.stringify(json),
        success: function(data) {
          alert("Widget exported.");
        },
        fail: function(data) {
          alert("Widget export failed.");
        }
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
            <select id="table" size="10" onMouseUp="getFamilies()" style="min-width: 100px;" class="select">
            </select>
          </td>
          <td>
            Column Family<br>
            <select id="family" multiple size="10" style="min-width: 110px;" onMouseUp="getColumns()">
            </select>
          </td>
          <td>
            Column<br>
            <select id="column" multiple size="10" style="min-width: 100px;" onMouseUp="getRows()">
            </select>
          </td>
          <td>
            Row<br>
            <select id="row" size="10" style="min-width: 100px;">
            </select>
          </td>
          <td>
            <table>
              <tr>
                <td>
                  <label>Y-axis Min</label>
                </td>
                <td>
                  <input type="text" id="ymin" />
                </td>
              </tr>
              <tr>
                <td>
                  <label>Y-axis Max</label>
                </td>
                <td>
                  <input type="text" id="ymax" />
                </td>
              </tr>
            </table>
          </td>
        </tr>
        <tr>
          <td>
            <input type=button name="action" value="Plot" onClick="plot()">
            <input type=button name="action" value="Export" onClick="exportWidget()">
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

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
var pause=false;
var timer;
var bound=null;
var _chartSeriesSize=0;

/*
 * calculcate the min, max, average of the data set
 */
function getStatis(data) {
  var first=true;
  var min=0;
  var max=0;
  var average=0;
  var sum=0;
  var count=0;
  var last=0;
  var sum_sqr=0.0;
  for (i=0; i<data.length;i++) {
    try {
      x=data[i][0];
      y=data[i][1];
    } catch(nullException) {
      continue;
    }
    if (bound!=null) {
      if (!((x >= bound.xmin) && (x <=bound.xmax) &&
	  (y >= bound.ymin) && (y <=bound.ymax))) {
        continue;
      }
    }
    if  (first) {
      min=y;
      max=y;
      first=false;
    } else {
      if (y < min) {
        min=y;
      }
      if (y > max) {
        max=y;
      }
    }
    sum+=y;
    count+=1;
    last=y;
    sum_sqr=sum_sqr+Math.pow(y,2);
  }
  average=sum/count;
  stdev=Math.sqrt(sum_sqr/count-Math.pow(average,2));
  return {min: min, max: max, average: average, stdev: stdev, sum: sum, count: count, last: last};
}

/*
 * format the float number to display the specified precision 
 */
function toFixed(value, precision) {
  var power = Math.pow(10, precision || 0);
  return String(Math.round(value * power) / power);
}

/*
 * create the legend table to display the max, min, average for each data series.
 */
function calculateStatis() {
  var dataTable='';
  dataTable+='<br/>';
  dataTable+='<table class="statisTable">';
  dataTable+='<thead>';
  dataTable+='<tr><th>Series</th><th>Maximum</th><th>Average</th><th>Minimum</th><th>St. Deviation</th><th>Last</th></tr>';  
  dataTable+='</thead>';
  for (j = 0; j < _series.length; ++j) {
    statis = getStatis(_series[j].data);
    row_class = ((j%2)==0)?'row-a':'row-b';
    dataTable+='<tr class="'+row_class+'"><td>'+_series[j].label+'</td>';
    dataTable+='<td align="right">'+toFixed(statis.max,2)+'</td>';
    dataTable+='<td align="right">'+toFixed(statis.average,2)+'</td>';
    dataTable+='<td align="right">'+toFixed(statis.min,2)+'</td>';
    dataTable+='<td align="right">'+toFixed(statis.stdev,2)+'</td>';
    dataTable+='<td align="right">'+toFixed(statis.last,2)+'</td>';
    dataTable+='</tr>';
  }
  dataTable+='</table>';
  $('#statisLegend').html(dataTable);
}

/*
 * setup tooltip
 */
function showTooltip(x, y, contents) {
  if(x>document.body.clientWidth*.6) {
    x=x-50;
  }
  if(y>document.body.clientHeight*.7) {
    y=y-40;
  }
  $('<div id="tooltip">' + contents + '</div>').css( {
    position: 'absolute',
    display: 'none',
    top: y + 5,
    left: x + 5,
    'border-radius': '5px',
    border: '2px solid #496274',
    padding: '2px',
    'color': '#fff',
    'background-color': '#496274',
    'opacity': '0.9'
  }).appendTo("body").fadeIn(200);
}

/*
 * calculate the height of the area and set the correct height for the chart, legend and the statis legend as well.
 */
function wholePeriod() {
  var ch = document.body.clientHeight;
  if (ch < 200 ) {
    $('#placeholderLegend').hide();
    $('#statisLegend').hide();
  } else if (ch < 320) {
    $('#placeholderLegend').show();
    $('#statisLegend').hide();
  } else {
    $('#placeholderLegend').show();
    $('#statisLegend').show();
  }
  $.plot($("#placeholder"), _series, _options);
  // update statis
  calculateStatis();
};

/*
 * predefined option of the chart
 */
options={
 points: { show: true },
 xaxis: {
   mode: "time"
 },
 selection: { mode: "xy" },
 grid: {
   hoverable: true,
   clickable: true,
   tickColor: "#C0C0C0",
   backgroundColor:"#FFFFFF"
 },
 legend: { show: false }
};

/*
 * bind the function for the hightlight the point in the chart.
 */
var previousPoint = null;
$("#placeholder").bind("plothover", function (event, pos, item) {
    var leftPad = function(n) {
      n = "" + n;
      return n.length == 1 ? "0" + n : n;
    };
    if (item) {
      var contents = "<div style='width:14px;height:10px;background-color:"+item.series.color+";overflow:hidden;display:inline-block;'></div>";
      if (previousPoint != item.datapoint) {
        previousPoint = item.datapoint;
        pause = true;
        $("#tooltip").remove();
        var x = item.datapoint[0],
          y = item.stackValue.toFixed(2);
        showTooltip(item.pageX, item.pageY,
          contents + " " + y);
      }
    } else {
      $("#tooltip").remove();
      previousPoint = null;
      pause = false;
    }
  });

/*
 * bind the function for resizing the area inside the chart.
 */
$("#placeholder").bind("selected", function (event, area) {
    if(area.x1 == area.x2 && area.y1 == area.y2) {
      pause = false;
    } else {
      pause = true;
    }
    extra_options = {};
    extra_options.xaxis={ min: area.x1, max: area.x2 };
    extra_options.yaxis={ min: area.y1, max: area.y2 };
    bound = {};
    bound.xmin=area.x1;
    bound.xmax=area.x2;
    bound.ymin=area.y1;
    bound.ymax=area.y2;
    calculateStatis();
    plot = $.plot(
      $("#placeholder"),
      _series,
      $.extend(
         true, 
         {}, 
         _options, extra_options
         )
      );
  });

/*
 * addept iframe height to content height
 */
function getDocHeight(doc) {
  var docHt = 0, sh, oh;
  if (doc.height) docHt = doc.height;
  else if (doc.body) {
    if (doc.body.scrollHeight) docHt = sh = doc.body.scrollHeight;
    if (doc.body.offsetHeight) docHt = oh = doc.body.offsetHeight;
    if (sh && oh) docHt = Math.max(sh, oh);
  }
  return docHt;
}

/*
 * Reload data
 */
function reload() {
  if (!pause) {
    loadData();
  }
  timer = setTimeout(reload, 3000);
}

/*
 * Initialize data from REST API.
 */
function loadData() {
  $.ajax({
    url: '/hicc/v1/chart/preview/series',
    type: 'PUT',
    contentType: 'application/json',
    data: JSON.stringify(_seriesTemplate),
    dataType: "json",
    success: function(result) {
      _series = result;
      wholePeriod();
    }
  });
}

/*
 * Generate static image report
 */ 
function saveReport() {
  var pattern = /https?:\/\/(.*?)\//;
  var baseUrl = pattern.exec(location.href)[0];
  var chart = document.getElementById('placeholder').cloneNode(true);
  var canvas = document.getElementById('placeholder').getElementsByTagName('canvas')[0];
  chart.getElementsByTagName('canvas')[0].style.display="none";
  chart.getElementsByTagName('canvas')[1].style.display="none";
  var legend = document.getElementById('placeholderLegend');
  var tableLegend = document.getElementById('statisLegend');
  var data="data:text/html,";
  data+="<!doctype html>";
  data+="<link type=\"text/css\" rel=\"stylesheet\" href=\""+baseUrl+"hicc/css/default.css\" />";
  data+="<link type=\"text/css\" rel=\"stylesheet\" href=\""+baseUrl+"hicc/css/iframe.css\" />";
  data+="<link type=\"text/css\" rel=\"stylesheet\" href=\""+baseUrl+"hicc/css/flexigrid/flexigrid.css\" />";
  data+="<center>";
  data+="<div id=\"placeholder\" style=\"width:"+chart.style.width+"; height:"+chart.style.height+"; position: relative;\">";
  data+="<img src=\""+canvas.toDataURL()+"\" style=\"position: absolute; left: 0px; top: 0px\">";
  data+="<center>";
  data+=chart.innerHTML;
  data+="</center>";
  data+="</div>";
  data+=legend.innerHTML;
  data+=tableLegend.innerHTML;
  data+="</center>";
  return window.open(data);
}

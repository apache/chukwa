var zoom=false;
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
  dataTable+='<table class="statisTable small_font">';
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
  setIframeHeight(document.getElementById('boxId').value);
}

/*
 * setup tooltip
 */
function showTooltip(x, y, contents) {
  if(x>document.body.clientWidth*.6) {
    x=x-200;
  }
  if(y>document.body.clientHeight*.7) {
    y=y-40;
  }
  $('<div id="tooltip">' + contents + '</div>').css( {
    position: 'absolute',
	display: 'none',
	top: y + 5,
	left: x + 5,
	border: '2px solid #aaa',
	padding: '2px',
	'background-color': '#fff',
        }).appendTo("body").fadeIn(200);
}

/*
 * calculate the height of the area and set the correct height for the chart, legend and the statis legend as well.
 */
function wholePeriod() {
  var cw = document.body.clientWidth-30;
  var ch = height-$("#placeholderTitle").height()-10;
  document.getElementById('placeholder').style.width=cw+'px';
  document.getElementById('placeholder').style.height=ch+'px';
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
   hoverable: false,
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
$("#placeholder").bind("plotclick", function (event, pos, item) {
    var leftPad = function(n) {
      n = "" + n;
      return n.length == 1 ? "0" + n : n;
    };
    if (item) {
      if (previousPoint != item.datapoint) {
	previousPoint = item.datapoint;
               
	$("#tooltip").remove();
	if(xLabels.length==0) {
	  var x = item.datapoint[0],
	    y = item.stackValue.toFixed(2);
	  var dnow=new Date();
	  dnow.setTime(x);
	  var dita=leftPad(dnow.getUTCFullYear())+"/"+leftPad(dnow.getUTCMonth()+1)+"/"+dnow.getUTCDate()+" "+leftPad(dnow.getUTCHours())+":"+leftPad(dnow.getUTCMinutes())+":"+leftPad(dnow.getUTCSeconds());
 
	  showTooltip(item.pageX, item.pageY,
		      item.series.label + ": " + y + "<br>Time: " + dita);
	} else {
	  var x = item.datapoint[0],
	    y = item.stackValue.toFixed(2);
	  xLabel = xLabels[x];
	  showTooltip(item.pageX, item.pageY,
		      item.series.label + ": " + y + "<br>" + xLabel);
	}
      }
    } else {
      $("#tooltip").remove();
      previousPoint = null;            
    }
  });

/*
 * bind the function for resizing the area inside the chart.
 */
$("#placeholder").bind("selected", function (event, area) {
    zoom = true;
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
    setIframeHeight(document.getElementById('boxId').value);
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
 * auto resize the iframe height to match content.
 */
function setIframeHeight(ifrm) {
  try {
    objToResize = parent.document.getElementById(window.name);
    objToResize.height = document.body.scrollHeight;
  } catch(err) {
    window.status = err.message;
  }
}

/*
 * refresh the chart widget.
 */
function refresh(url, parameters) {
  bound=null;
  if(zoom) {
    wholePeriod();
    zoom=false;
  } else {
    if(parameters.indexOf("render=stack")>0) {
      return false;
    }
    if(parameters.indexOf("_force_refresh")>0) {
      return false;
    }
    var dataURL = url+"?"+parameters;
    $.get(dataURL,{format: 'json'}, function(data){
      try {
        eval(data);
        wholePeriod();
        document.getElementById('placeholderTitle').innerHTML=chartTitle;
      } catch(err) {
        return false;
      }
    });a
    if(_rest!=null) {
      loadData(url, parameters);
    }
  }
  return true;
}

/*
 * Initialize data from REST API.
 */
function loadData() {
  _chartSeriesSize=0;
  for(var i=0;i<_series.length;i++) {
    $.getJSON(_rest[i], function(json) {
      if(json.constructor.toString().indexOf("Array") != -1) {
        for(var index=0;index<json.length;index++) {
          _series[_chartSeriesSize].label=json[index].name;
          _series[_chartSeriesSize].data=json[index].data;
          _chartSeriesSize++;
        }
      } else {
        var name=json.name;
        _series[_chartSeriesSize].label=name;
        _series[_chartSeriesSize].data=json.data;
        _chartSeriesSize++;
      }
      wholePeriod();
    }); 
  }
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

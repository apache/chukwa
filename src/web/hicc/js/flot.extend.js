var zoom=false;

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

function wholePeriod() {
        var cw = document.body.clientWidth-30;
        var ch = height-$("#placeholderTitle").height()-10;
        document.getElementById('placeholder').style.width=cw+'px';
        document.getElementById('placeholder').style.height=ch+'px';
        $.plot($("#placeholder"), _series, _options);
        setIframeHeight(document.getElementById('boxId').value, height+$("#placeholderTitle").height()+$("#placeholderLegend").height());
};

options={
        points: { show: true },
        xaxis: {                timeformat: "%y/%O/%D<br/>%H:%M:%S",
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
	$("#placeholder").bind("selected", function (event, area) {
            zoom = true;
			plot = $.plot(
				$("#placeholder"),
				_series,
				$.extend(
					true, 
					{}, 
					_options, {
						xaxis: { min: area.x1, max: area.x2 }
					}
				)
			);
		});

//  addept iframe height to content height
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

function setIframeHeight(ifrm, height) {
  try {
    frame = window.parent.document.getElementById(ifrm);
    innerDoc = (frame.contentDocument) ? frame.contentDocument : frame.contentWindow.document;
    objToResize = (frame.style) ? frame.style: frame;
     if(height==0) {
       objToResize.height = innerDoc.body.scrollHeight;
     } else {
       objToResize.height = height;
     }
  } catch(err) {
    window.status = err.message;
  }
}

function refresh(url, parameters) {
  if(zoom) {
    wholePeriod();
    zoom=false;
  } else {
    if(parameters.indexOf("render=stack")>0) {
        throw "force iframe refresh";
    }
    var dataURL = url+"?"+parameters;
    $.get(dataURL,{format: 'json'}, function(data){
        eval(data);
        wholePeriod();
        document.getElementById('placeholderTitle').innerHTML=chartTitle;
    });
  }
}

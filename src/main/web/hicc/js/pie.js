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
var pieChart = null;
var labels = [];
var series = [];

for (var i = 0; i < _series.length; i++) {
  labels[i] = _series[i].label;
  series[i] = 1;
}

var data = {
  labels: labels,
  series: series
};

var options = {
  donut: donut,
  labelInterpolationFnc: function(value) {
    return value[0]
  }
};

var responsiveOptions = [
  ['screen and (min-width: 200px)', {
    chartPadding: 20,
    labelOffset: 20,
    labelDirection: 'explode',
    labelInterpolationFnc: function(value) {
      return value;
    }
  }],
  ['screen and (min-width: 400px)', {
    labelOffset: 80,
    chartPadding: 20
  }]
];

var timer = null;

function refresh() {
  var buffer=JSON.stringify(_series);
  $.ajax({
    url: "/hicc/v1/piechart/preview/series",
    type: 'PUT',
    contentType: 'application/json',
    data: buffer,
    dataType: 'json',
    success: function(result) {
      var data = {
        labels: labels,
        series: result
      };
      if(pieChart == null) {
        pieChart = new Chartist.Pie('.ct-chart', data, options, responsiveOptions);
        timer = setTimeout(refresh, 3000);
      } else {
        pieChart.update(data, options, true);
      }
    }
  });
}

refresh();

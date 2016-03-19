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

function getParameterByName(name) {
    name = name.replace(/[\[]/, "\\[").replace(/[\]]/, "\\]");
    var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
        results = regex.exec(location.search);
    return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

Date.prototype.addHours= function(h){
    this.setHours(this.getHours()+h);
    return this;
}

function getDateStr(date) {
  var str = date.getUTCFullYear() + '-' + (date.getUTCMonth()+1) + '-' + date.getUTCDate() + 'T' +
    date.getUTCHours() + ':' + date.getUTCMinutes() + ':' + date.getUTCSeconds() + '.' + date.getUTCMilliseconds() + 'Z';
  return str;
}

function getLog(type, startDate, direction) {
  var q="";
  var startDateStr = getDateStr(startDate);
  if(direction=='up') {
    _top.addHours(-1);
    var lastHour = getDateStr(_top);
    q='+type:\"'+type+'\"+source:\"'+source+'\"+date:['+lastHour+' TO '+startDateStr+']';
  } else {
    _bottom.addHours(1);
    var nextHour = getDateStr(_bottom);
    q='+type:\"'+type+'\"+source:\"'+source+'\"+date:['+startDateStr+' TO '+nextHour+']';
  }
  console.log(q);
  $.ajax({
    url: '/hicc/solr/chukwa/select',
    data: {
      q: q,
      wt: "json",
      sort: "date asc",
      start: 0,
      rows: 1000
    },
    dataType:'json',
    success: function(data) {
      var content = [];
      $.each(data.response.docs, function(index, value) {
        content[index]=value.data;
      });
      if(direction=='up') {
        $('#content').prepend(content.join(""));
      } else {
        $('#content').append(content.join(""));
      }
    },
    error: function(data) {
      console.log(data);
    }
  });
}

var today = new Date();
var type = getParameterByName("type") || "HadoopNNLog";
var startDate = new Date(getParameterByName("date")) || today;
var source = getParameterByName("source") || "localhost";
var _top = new Date(startDate.getTime());
var _bottom = new Date(startDate.getTime());

getLog(type, startDate, "down");

$(window).scroll(function() {
 if($(window).scrollTop() + $(window).height() == $(document).height()) {
   getLog(type, _bottom, "down");
 } else if($(window).scrollTop() == 0) {
   if(_top>0) {
     getLog(type, _top, "up");
   }
 }
});

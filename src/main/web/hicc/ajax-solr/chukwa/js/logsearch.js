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
var Manager;

require.config({
  paths: {
    core: '../../core',
    managers: '../../managers',
    widgets: '../../widgets',
    logsearch: '../widgets'
  },
  urlArgs: "bust=" +  (new Date()).getTime()
});

(function ($) {

define([
  'managers/Manager.jquery',
  'core/ParameterStore',
  'logsearch/ResultWidget',
  'logsearch/TagcloudWidget',
  'logsearch/CurrentSearchWidget.9',
  'logsearch/AutocompleteWidget',
  'logsearch/CountryCodeWidget',
  'logsearch/CalendarWidget',
  'widgets/jquery/PagerWidget'
], function () { 

  $(function () {
    Manager = new AjaxSolr.Manager({
      solrUrl: '/hicc/solr/chukwa/'
    });
    Manager.addWidget(new AjaxSolr.ResultWidget({
      id: 'result',
      target: '#docs'
    }));
    Manager.addWidget(new AjaxSolr.PagerWidget({
      id: 'pager',
      target: '#pager',
      prevLabel: '&lt;',
      nextLabel: '&gt;',
      innerWindow: 1,
      renderHeader: function (perPage, offset, total) {
        $('#pager-header').html($('<span></span>').text('displaying ' + Math.min(total, offset + 1) + ' to ' + Math.min(total, offset + perPage) + ' of ' + total));
      }
    }));
    var fields = [ 'type', 'service', 'source', 'data', 'user' ];
    for (var i = 0, l = fields.length; i < l; i++) {
      Manager.addWidget(new AjaxSolr.TagcloudWidget({
        id: fields[i],
        target: '#' + fields[i],
        field: fields[i]
      }));
    }
    Manager.addWidget(new AjaxSolr.CurrentSearchWidget({
      id: 'currentsearch',
      target: '#selection'
    }));
    Manager.addWidget(new AjaxSolr.AutocompleteWidget({
      id: 'text',
      target: '#search',
      fields: fields
    }));
    Manager.addWidget(new AjaxSolr.CountryCodeWidget({
      id: 'countries',
      target: '#countries',
      field: 'countryCodes'
    }));
    Manager.addWidget(new AjaxSolr.CalendarWidget({
      id: 'calendar',
      target: '#calendar',
      field: 'date'
    }));
    Manager.init();
    Manager.store.addByValue('q', '*:*');
    var today = new Date();
    var todayStr = today.getUTCFullYear() + '-' + (today.getUTCMonth()+1) + '-' + today.getUTCDate() + 'T' +
      today.getUTCHours() + ':' + today.getUTCMinutes() + ':' + today.getUTCSeconds() + '.' + today.getUTCMilliseconds() + 'Z';
    var params = {
      facet: true,
      'facet.field': [ 'type', 'service', 'source', 'data', 'user' ],
      'facet.limit': 20,
      'facet.mincount': 1,
      'f.type.facet.limit': 20,
      'f.source.facet.limit': 20,
      'f.data.facet.limit': 20,
      'f.user.facet.limit': 20,
      'facet.date': 'date',
      'facet.date.start': todayStr + '/DAY',
      'facet.date.end': todayStr + '/DAY+1DAY',
      'facet.date.gap': '+1DAY',
      'json.nl': 'map'
    };
    for (var name in params) {
      Manager.store.addByValue(name, params[name]);
    }
    Manager.doRequest();
  });

  $.fn.showIf = function (condition) {
    if (condition) {
      return this.show();
    }
    else {
      return this.hide();
    }
  }
});

})(jQuery);

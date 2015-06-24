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
(function (callback) {
  if (typeof define === 'function' && define.amd) {
    define(['core/AbstractManager'], callback);
  }
  else {
    callback();
  }
}(function () {

/**
 * @see http://wiki.apache.org/solr/SolJSON#JSON_specific_parameters
 * @class Manager
 * @augments AjaxSolr.AbstractManager
 */
AjaxSolr.Manager = AjaxSolr.AbstractManager.extend(
  /** @lends AjaxSolr.Manager.prototype */
  {
  executeRequest: function (servlet, string, handler, errorHandler) {
    var self = this,
        options = {dataType: 'json'};
    string = string || this.store.string();
    handler = handler || function (data) {
      self.handleResponse(data);
    };
    errorHandler = errorHandler || function (jqXHR, textStatus, errorThrown) {
      self.handleError(textStatus + ', ' + errorThrown);
    };
    if (this.proxyUrl) {
      options.url = this.proxyUrl;
      options.data = {query: string};
      options.type = 'POST';
    }
    else {
      options.url = this.solrUrl + servlet + '?' + string + '&wt=json&json.wrf=?';
    }
    jQuery.ajax(options).done(handler).fail(errorHandler);
  }
});

}));

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
    define(['core/AbstractTextWidget'], callback);
  }
  else {
    callback();
  }
}(function () {

(function ($) {

AjaxSolr.AutocompleteWidget = AjaxSolr.AbstractTextWidget.extend({
  afterRequest: function () {
    $(this.target).find('input').unbind().removeData('events').val('');

    var self = this;

    var list = [];
    for (var i = 0; i < this.fields.length; i++) {
      var field = this.fields[i];
      for (var facet in this.manager.response.facet_counts.facet_fields[field]) {
        list.push({
          field: field,
          value: facet,
          label: facet + ' (' + this.manager.response.facet_counts.facet_fields[field][facet] + ') - ' + field
        });
      }
    }

    this.requestSent = false;
    $(this.target).find('input').autocomplete('destroy').autocomplete({
      source: list,
      select: function(event, ui) {
        if (ui.item) {
          self.requestSent = true;
          if (self.manager.store.addByValue('fq', ui.item.field + ':' + AjaxSolr.Parameter.escapeValue(ui.item.value))) {
            self.doRequest();
          }
        }
      }
    });

    // This has lower priority so that requestSent is set.
    $(this.target).find('input').bind('keydown', function(e) {
      if (self.requestSent === false && e.which == 13) {
        var value = $(this).val();
        if (value && self.set(value)) {
          self.doRequest();
        }
      }
    });
  }
});

})(jQuery);

}));

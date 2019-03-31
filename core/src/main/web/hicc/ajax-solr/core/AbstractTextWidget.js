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
    define(['core/AbstractWidget'], callback);
  }
  else {
    callback();
  }
}(function () {

/**
 * Baseclass for all free-text widgets.
 *
 * @class AbstractTextWidget
 * @augments AjaxSolr.AbstractWidget
 */
AjaxSolr.AbstractTextWidget = AjaxSolr.AbstractWidget.extend(
  /** @lends AjaxSolr.AbstractTextWidget.prototype */
  {
  /**
   * @param {Object} [attributes]
   * @param {Number} [attributes.start] This widget will by default set the
   *   offset parameter to 0 on each request.
   */
  constructor: function (attributes) {
    AjaxSolr.AbstractTextWidget.__super__.constructor.apply(this, arguments);
    AjaxSolr.extend(this, {
      start: 0
    }, attributes);
  },

  /**
   * Sets the main Solr query to the given string.
   *
   * @param {String} q The new Solr query.
   * @returns {Boolean} Whether the selection changed.
   */
  set: function (q) {
    return this.changeSelection(function () {
      this.manager.store.get('q').val(q);
    });
  },

  /**
   * Sets the main Solr query to the empty string.
   *
   * @returns {Boolean} Whether the selection changed.
   */
  clear: function () {
    return this.changeSelection(function () {
      this.manager.store.remove('q');
    });
  },

  /**
   * Helper for selection functions.
   *
   * @param {Function} Selection function to call.
   * @returns {Boolean} Whether the selection changed.
   */
  changeSelection: function (func) {
    var before = this.manager.store.get('q').val();
    func.apply(this);
    var after = this.manager.store.get('q').val();
    if (after !== before) {
      this.afterChangeSelection(after);
    }
    return after !== before;
  },

  /**
   * An abstract hook for child implementations.
   *
   * <p>This method is executed after the main Solr query changes.</p>
   *
   * @param {String} value The current main Solr query.
   */
  afterChangeSelection: function (value) {},

  /**
   * Returns a function to unset the main Solr query.
   *
   * @returns {Function}
   */
  unclickHandler: function () {
    var self = this;
    return function () {
      if (self.clear()) {
        self.doRequest();
      }
      return false;
    }
  },

  /**
   * Returns a function to set the main Solr query.
   *
   * @param {String} value The new Solr query.
   * @returns {Function}
   */
  clickHandler: function (q) {
    var self = this;
    return function () {
      if (self.set(q)) {
        self.doRequest();
      }
      return false;
    }
  }
});

}));

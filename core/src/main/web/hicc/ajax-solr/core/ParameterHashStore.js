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
    define(['core/ParameterStore'], callback);
  }
  else {
    callback();
  }
}(function () {

/**
 * A parameter store that stores the values of exposed parameters in the URL
 * hash to maintain the application's state.
 *
 * <p>The ParameterHashStore observes the hash for changes and loads Solr
 * parameters from the hash if it observes a change or if the hash is empty.
 * The onhashchange event is used if the browser supports it.</p>
 *
 * <p>Configure the manager with:</p>
 *
 * @class ParameterHashStore
 * @augments AjaxSolr.ParameterStore
 * @see https://developer.mozilla.org/en-US/docs/DOM/window.onhashchange
 */
AjaxSolr.ParameterHashStore = AjaxSolr.ParameterStore.extend(
  /** @lends AjaxSolr.ParameterHashStore.prototype */
  {
  /**
   * @param {Object} [attributes]
   * @param {Number} [attributes.interval] The interval in milliseconds to use
   *   in <tt>setInterval()</tt>. Do not set the interval too low as you may set
   *   up a race condition. Defaults to 250.
   */
  constructor: function (attributes) {
    AjaxSolr.ParameterHashStore.__super__.constructor.apply(this, arguments);
    AjaxSolr.extend(this, {
      interval: 250,
      // Reference to the setInterval() function.
      intervalId: null,
      // A local copy of the URL hash, so we can detect changes to it.
      hash: ''
    }, attributes);
  },

  /**
   * If loading and saving the hash take longer than <tt>interval</tt>, we'll
   * hit a race condition. However, this should never happen.
   */
  init: function () {
    if (this.exposed.length) {
      // Check if the browser supports the onhashchange event. IE 8 and 9 in compatibility mode
      // incorrectly report support for onhashchange.
      if ('onhashchange' in window && (!document.documentMode || document.documentMode > 7)) {
        if (window.addEventListener) {
          window.addEventListener('hashchange', this.intervalFunction(this), false);
        }
        else if (window.attachEvent) {
          window.attachEvent('onhashchange', this.intervalFunction(this));
        }
        else {
          window.onhashchange = this.intervalFunction(this);
        }
      }
      else {
        this.intervalId = window.setInterval(this.intervalFunction(this), this.interval);
      }
    }
  },

  /**
   * Stores the values of the exposed parameters in both the local hash and the
   * URL hash. No other code should be made to change these two values.
   */
  save: function () {
    this.hash = this.exposedString();
    if (this.storedString()) {
      // Make a new history entry.
      window.location.hash = this.hash;
    }
    else {
      // Replace the old history entry.
      window.location.replace(window.location.href.replace('#', '') + '#' + this.hash);
    }
  },

  /**
   * @see ParameterStore#storedString()
   */
  storedString: function () {
    // Some browsers automatically unescape characters in the hash, others
    // don't. Fortunately, all leave window.location.href alone. So, use that.
    var index = window.location.href.indexOf('#');
    if (index == -1) {
      return '';
    }
    else {
      return window.location.href.substr(index + 1);
    }
  },

  /**
   * Checks the hash for changes, and loads Solr parameters from the hash and
   * sends a request to Solr if it observes a change or if the hash is empty
   */
  intervalFunction: function (self) {
    return function () {
      // Support the back/forward buttons. If the hash changes, do a request.
      var hash = self.storedString();
      if (self.hash != hash && decodeURIComponent(self.hash) != decodeURIComponent(hash)) {
        self.load();
        self.manager.doRequest();
      }
    }
  }
});

}));

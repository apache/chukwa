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
;(function(history) {
  /**
   * A parameter store that stores the values of exposed parameters in the URL via History.js
   * to maintain the application's state. This uses the HTML5 History API for newer browsers, and
   * falls back to using the hash in older browsers. Don't forget to add the following (or similar)
   * inside your <tt>head</tt> tag:
   *
   * <pre>
   * <script src="history.js/scripts/bundled/html4+html5/jquery.history.js"></script>
   * </pre>
   *
   * Configure the manager with:
   *
   * <pre>
   * Manager.setStore(new AjaxSolr.ParameterHistoryStore());
   * </pre>
   *
   * @class ParameterHistoryStore
   * @augments AjaxSolr.ParameterStore
   * @see https://github.com/browserstate/history.js
   * @see http://www.whatwg.org/specs/web-apps/current-work/multipage/history.html
   */
  AjaxSolr.ParameterHistoryStore = AjaxSolr.ParameterStore.extend(
    /** @lends AjaxSolr.ParameterHistoryStore.prototype */
    {
    init: function () {
      if (this.exposed.length) {
        if (!history) {
          throw 'ParameterHistoryStore requires History.js';
        }

        history.Adapter.bind(window, 'statechange', this.stateChangeFunction(this));
      }
    },

    /**
     * Stores the values of the exposed parameters in both the local hash and History.js
     * No other code should be made to change these two values.
     */
    save: function () {
      this.hash = this.exposedString();
      history.pushState({ params: this.hash }, null, '?' + this.hash);
    },

    /**
     * @see ParameterStore#storedString()
     */
    storedString: function () {
      var state = history.getState();

      // Load the state from the History object.
      if (state.data && state.data.params) {
        return state.data.params;
      }

      // If initial load, load the state from the URL.
      var url = state.cleanUrl, index = url.indexOf('?');
      if (index == -1) {
        return '';
      }
      else {
        return url.substr(index + 1);
      }
    },

    /**
     * Called when History.js detects a state change. Checks if state is different to previous state,
     * and if so, sends a request to Solr. This needs to check if the state has changed since it also
     * gets called when we call pushState above.
     */
    stateChangeFunction: function (self) {
      return function () {
        var hash = self.storedString();

        if (self.hash != hash) {
          self.load();
          self.manager.doRequest();
        }
      }
    }
  });
})(window.History);

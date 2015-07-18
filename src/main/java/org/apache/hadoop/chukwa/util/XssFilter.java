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

package org.apache.hadoop.chukwa.util;

import javax.servlet.http.HttpServletRequest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;
import org.owasp.esapi.ESAPI;

public class XssFilter {
    private HttpServletRequest request = null;
    private static Log LOG = LogFactory.getLog(XssFilter.class);

    public XssFilter() {
    }

    public XssFilter(HttpServletRequest request) {
      // Return the cleansed request
      this.request = request;
    }
    
    public String getParameter(String key) {
      String value=null;
      try {
        value=filter(this.request.getParameter(key));
      } catch (Exception e) {
        LOG.info("XssFilter.getParameter: Cannot get parameter for: "+key);
      }
      return value;
    }
    
    public String[] getParameterValues(String key) {
      String[] values=null;
      try {
	  values  = this.request.getParameterValues(key);
          int i = 0;
          for(String value : values) {
            values[i] = filter(value);
            i++;
          }
      } catch (Exception e) {
	  LOG.info("XssFilter.getParameterValues: cannot get parameter for: "+key);
      }
      return values;
    }

    /**
     * Strips any potential XSS threats out of the value
     * @param value
     * @return
     */
    public String filter( String value ) {
      if( value == null )
        return null;
     
      // Use the ESAPI library to avoid encoded attacks.
      value = ESAPI.encoder().canonicalize( value );
 
      // Avoid null characters
      value = value.replaceAll("\0", "");
 
      // Clean out HTML
      value = Jsoup.clean( value, Whitelist.none() );
 
      return value;
    }
}

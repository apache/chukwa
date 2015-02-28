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

import java.util.Enumeration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;
import org.owasp.esapi.ESAPI;

import javax.ws.rs.core.MultivaluedMap;

public class XssFilter {
    private HttpServletRequest request = null;
    private static Log LOG = LogFactory.getLog(XssFilter.class);
    private HttpSession session = null;

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
     * Apply the XSS filter to the parameters
     * @param parameters
     * @param type
     */
    private void cleanParams( MultivaluedMap<String, String> parameters ) {
      for( Map.Entry<String, List<String>> params : parameters.entrySet() ) {
        String key = params.getKey();
        List<String> values = params.getValue();
 
        List<String> cleanValues = new ArrayList<String>();
        for( String value : values ) {
          cleanValues.add( filter( value ) );
        }
 
        parameters.put( key, cleanValues );
      }
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
